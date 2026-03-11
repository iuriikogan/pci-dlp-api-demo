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
    def __init__(self, project_id, kms_key_name, wrapped_key, headers):
        self.project_id = project_id
        self.kms_key_name = kms_key_name
        self.wrapped_key = wrapped_key
        self.headers = headers
        self.dlp_client = None

    def start_bundle(self):
        if self.dlp_client is None:
            self.dlp_client = dlp_v2.DlpServiceClient()

    def process(self, batch):
        if not batch:
            return

        try:
            parent = f"projects/{self.project_id}/locations/global"
            
            # 1. Parse raw CSV lines into Table cells dynamically
            rows = []
            for line in batch:
                parsed_line = next(csv.reader([line]))
                cells = [{"string_value": str(val).strip()} for val in parsed_line]
                rows.append({"values": cells})
            
            table = {"headers": [{"name": h} for h in self.headers], "rows": rows}

            # 2. Bulletproof Alphabets
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

            # 3. DYNAMIC DISCOVERY: Define what the engine should hunt for
            inspect_config = {
                "info_types": [
                    {"name": "EMAIL_ADDRESS"},
                    {"name": "PERSON_NAME"},
                    {"name": "CREDIT_CARD_NUMBER"},
                    {"name": "US_SOCIAL_SECURITY_NUMBER"},
                    {"name": "PHONE_NUMBER"}
                ]
            }

            # 4. Map the discovered InfoTypes to the correct FPE alphabet
            info_type_transformations = {
                "transformations": [
                    {
                        "info_types": [{"name": "EMAIL_ADDRESS"}, {"name": "PERSON_NAME"}],
                        "primitive_transformation": crypto_config_alphanumeric
                    },
                    {
                        "info_types": [{"name": "CREDIT_CARD_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"}, {"name": "PHONE_NUMBER"}],
                        "primitive_transformation": crypto_config_numeric
                    }
                ]
            }

            # 5. Call the API using BOTH inspect_config and deidentify_config
            response = self.dlp_client.deidentify_content(
                request={
                    "parent": parent,
                    "inspect_config": inspect_config,
                    "deidentify_config": {"info_type_transformations": info_type_transformations},
                    "item": {"table": table},
                }
            )

            # MAIN OUTPUT: Yield masked rows
            resp_table = response.item.table
            for row in resp_table.rows:
                values = [cell.string_value for cell in row.values]
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(values)
                yield output.getvalue().strip()

        except Exception as e:
            logging.error(f"Routing batch to DLQ due to DLP API Error: {e}")
            # DLQ OUTPUT: Yield the original, unmasked rows
            for line in batch:
                yield beam.pvalue.TaggedOutput('dlq', line)


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
    
    # New argument to make the script completely agnostic
    parser.add_argument("--csv_headers", required=True, help="Comma-separated list of headers in the CSV")
    
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        requirements_file=known_args.requirements_file,
        save_main_session=True
    )

    # Convert the string of headers into a Python list
    headers = known_args.csv_headers.split(",")

    with beam.Pipeline(options=pipeline_options) as p:
        batched_data = (
            p
            | "ReadCSV" >> beam.io.ReadFromText(known_args.input_bucket, skip_header_lines=1)
            # Notice we removed the ParseToDict step to save CPU. We just batch the raw lines!
            | "BatchRows" >> beam.BatchElements(min_batch_size=10, max_batch_size=100)
        )

        results = (
            batched_data
            | "Deidentify" >> beam.ParDo(DeidentifyCSVFn(
                project_id=known_args.project,
                kms_key_name=known_args.kms_key_name,
                wrapped_key=known_args.wrapped_key,
                headers=headers # Pass the dynamic headers into the worker
            )).with_outputs('dlq', main='main_output')
        )

        (
            results.main_output
            | "WriteSuccessToGCS" >> beam.io.WriteToText(
                known_args.output_bucket, 
                file_name_suffix=".csv",
                header=",".join(headers) # Inject the dynamic headers into every output file
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
