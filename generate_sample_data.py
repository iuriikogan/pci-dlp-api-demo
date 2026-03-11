import random
import csv

def generate_luhn_valid_cc():
    # Start with a known prefix: Visa (4), Mastercard (51-55), Amex (34, 37)
    prefix = random.choice(["4", "51", "52", "53", "54", "55", "34", "37"])
    length = 15 if prefix.startswith("3") else 16
    
    number = [int(x) for x in prefix]
    while len(number) < length - 1:
        number.append(random.randint(0, 9))
        
    checksum = 0
    for i, digit in enumerate(reversed(number)):
        if i % 2 == 0:
            doubled = digit * 2
            checksum += doubled - 9 if doubled > 9 else doubled
        else:
            checksum += digit
            
    check_digit = (10 - (checksum % 10)) % 10
    number.append(check_digit)
    return "".join(map(str, number))

def generate_ssn():
    # Basic SSN format (ignoring strictly invalid prefixes for simplicity, DLP usually flags any ###-##-####)
    return f"{random.randint(100, 999):03d}-{random.randint(10, 99):02d}-{random.randint(1000, 9999):04d}"

def generate_phone():
    return f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"

first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]

with open("sample_sensitive_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "first_name", "last_name", "email", "phone_number", "ssn", "credit_card", "customer_notes"])
    
    for i in range(1, 101):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        email = f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com"
        cc = generate_luhn_valid_cc()
        ssn = generate_ssn()
        phone = generate_phone()
        
        # Adding notes that explicitly mention sensitive data to trigger unstructured text DLP inspection
        notes_templates = [
            f"Please update billing for {cc}.",
            f"Customer called from {phone} regarding account ending in {cc[-4:]}.",
            f"Verify identity using SSN {ssn} before processing refund to {cc}.",
            f"Primary contact is {email}.",
            "Standard transaction."
        ]
        notes = random.choice(notes_templates)
        
        writer.writerow([
            f"TXN-{random.randint(10000, 99999)}",
            fn,
            ln,
            email,
            phone,
            ssn,
            cc,
            notes
        ])

print("Successfully generated sample_sensitive_data.csv with 100 records.")
