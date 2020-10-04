import csv

DELIMITER = chr(255)
data = ["itemA", "itemB", "itemC",
        "Sentence that might contain commas, colons: or even \"quotes\"."]

with open('data.csv', 'w', newline='') as outfile:
    writer = csv.writer(outfile, delimiter='|;|')
    writer.writerow(data)


