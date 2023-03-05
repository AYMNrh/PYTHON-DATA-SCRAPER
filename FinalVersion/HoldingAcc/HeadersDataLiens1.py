import csv

# Open the input CSV file and create a new output CSV file
with open('DataLiens.csv', 'r', encoding='utf-8', newline='') as infile, open('NewDataLiens.csv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # Iterate over each row in the input file
    for row in reader:
        # Check the number of columns in the row
        num_cols = len(row)
        if num_cols < 176:
            # Add empty values to make the row have 176 columns
            row += [''] * (176 - num_cols)
        elif num_cols > 176:
            # Trim the row to have 176 columns
            row = row[:176]

        # Write the modified row to the output file
        writer.writerow(row)



