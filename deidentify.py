import argparse
import logging
import base64
import csv
import io
import string
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import dlp_v2

class DeidentifyCSVFn(beam.DoFn):
    def __init__(self, project_id, kms_key_name, wrapped_key):
        self.project_id = project_id
        self.kms_key_name = kms_key_name
        self.wrapped_key = wrapped_key
        self.dlp_client = None

    def start_bundle(self):
        if self.dlp_client is None:
            self.dlp_client = dlp_v2.DlpServiceClient()

    def process(self, batch):
        if not batch:
            return
            
        headers = list(batch[0].keys())

        try:
            parent = f"projects/{self.project_id}/locations/global"
            
            rows = []
            for row in batch:
                # .strip() removes invisible \r, \n, and accidental spaces
                cells = [{"string_value": str(row[header]).strip()} for header in headers]
                rows.append({"values": cells})
            
            table = {"headers": [{"name": h} for h in headers], "rows": rows}

            # Max out the alphabet to the FFX limit (94 characters)
            # This includes all letters, numbers, punctuation, and a space.
            bulletproof_alphabet = string.ascii_letters + string.digits + string.punctuation + " "

            crypto_config_numeric = {
                "crypto_replace_ffx_fpe_config": {
                    "crypto_key": {
                        "kms_wrapped": {
                            "wrapped_key": base64.b64decode(self.wrapped_key),
                            "crypto_key_name": self.kms_key_name
                        }
                    },
                    "custom_alphabet": "0123456789- ", 
                }
            }

            crypto_config_alphanumeric = {
                "crypto_replace_ffx_fpe_config": {
                    "crypto_key": {
                        "kms_wrapped": {
                            "wrapped_key": base64.b64decode(self.wrapped_key),
                            "crypto_key_name": self.kms_key_name
                        }
                    },
                    "custom_alphabet": bulletproof_alphabet,
                }
            }

            transformations = [
                {"fields": [{"name": "first_name"}, {"name": "last_name"}, {"name": "email"}], "primitive_transformation": crypto_config_alphanumeric},
                {"fields": [{"name": "credit_card"}, {"name": "ssn"}, {"name": "phone_number"}], "primitive_transformation": crypto_config_numeric},
                {
                    "fields": [{"name": "customer_notes"}],
                    "info_type_transformations": {
                        "transformations": [{
                            "info_types": [{"name": "EMAIL_ADDRESS"}, {"name": "PHONE_NUMBER"}, {"name": "PERSON_NAME"}],
                            "primitive_transformation": crypto_config_alphanumeric
                        }]
                    }
                }
            ]

            response = self.dlp_client.deidentify_content(
                request={
                    "parent": parent,
                    "deidentify_config": {"record_transformations": {"field_transformations": transformations}},
                    "item": {"table": table},
                }
            )

            # 1. MAIN OUTPUT: Yield masked rows normally
            resp_table = response.item.table
            for row in resp_table.rows:
                values = [cell.string_value for cell in row.values]
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(values)
                yield output.getvalue().strip()

        except Exception as e:
            logging.error(f"Routing batch to DLQ due to DLP API Error: {e}")
            # 2. DLQ OUTPUT: Yield the original, unmasked rows using a TaggedOutput
            for row in batch:
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                writer.writerow([row[h] for h in headers])
                yield beam.pvalue.TaggedOutput('dlq', output.getvalue().strip())


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket", required=True)
    parser.add_argument("--output_bucket", required=True)
    parser.add_argument("--dlq_bucket", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--kms_key_name", required=True)
    parser.add_argument("--wrapped_key", required=True)
    parser.add_argument("--requirements_file", default="./requirements.txt")
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        requirements_file=known_args.requirements_file,
        save_main_session=True
    )

    headers = ["transaction_id", "first_name", "last_name", "email", "phone_number", "ssn", "credit_card", "customer_notes"]

    with beam.Pipeline(options=pipeline_options) as p:
        batched_data = (
            p
            | "ReadCSV" >> beam.io.ReadFromText(known_args.input_bucket, skip_header_lines=1)
            | "ParseToDict" >> beam.Map(lambda line: dict(zip(headers, next(csv.reader([line])))))
            | "BatchRows" >> beam.BatchElements(min_batch_size=10, max_batch_size=100)
        )

        results = (
            batched_data
            | "Deidentify" >> beam.ParDo(DeidentifyCSVFn(
                project_id=known_args.project,
                kms_key_name=known_args.kms_key_name,
                wrapped_key=known_args.wrapped_key
            )).with_outputs('dlq', main='main_output')
        )

        (
            results.main_output
            | "WriteSuccessToGCS" >> beam.io.WriteToText(
                known_args.output_bucket, 
                file_name_suffix=".csv",
                header=",".join(headers)
            )
        )

        (
            results.dlq
            | "WriteDLQToGCS" >> beam.io.WriteToText(
                known_args.dlq_bucket, 
                file_name_suffix="_dlq.csv",
                header=",".join(headers)
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
