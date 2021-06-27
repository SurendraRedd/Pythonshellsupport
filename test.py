import csv
import io
import zipfile


transactions = [
    {"f1": 1, "f2": 2, "f3": 3},
    {"f1": 3, "f2": 1, "f3": 2},
    {"f1": 2, "f2": 3, "f3": 1},
    {"f1": 4, "f2": 5, "f3": 6}
]


transactions_csv = io.StringIO()
writer = csv.DictWriter(transactions_csv, fieldnames=["f1", "f2", "f3"])
writer.writeheader()
for transaction in transactions:
    writer.writerow(transaction)
transactions_csv.seek(0)

with open("file.zip", "r+b") as z:
    new_zip = zipfile.ZipFile(z, "a", zipfile.ZIP_DEFLATED, False)
    new_zip.writestr("transaction4.csv", transactions_csv.read())
    new_zip.close()