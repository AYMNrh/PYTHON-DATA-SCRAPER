import csv

headers = [str(i) for i in range(1, 177)]  # create a list of headers from 1 to 176

# Open the CSV file in read and write mode
with open('NewDataLiens.csv', 'r+', encoding='utf-8', newline='') as csvfile:
    reader = csv.reader(csvfile)
    writer = csv.writer(csvfile)

    # Read the existing data and store it in a list
    data = [row for row in reader]

    # Go back to the beginning of the file and write the headers
    csvfile.seek(0)
    writer.writerow(headers)

    # Write the existing data after the headers
    writer.writerows(data)