import csv
from datetime import datetime

pwd = '/Users/edward_chiang/Downloads/'
date = datetime.today().strftime('%Y%m%d')

file_source = '%sM3%sexport.csv' % (pwd, date)
file_target1 = '%sM3_%s_contact.csv' % (pwd, date)
file_target2 = '%sM3_%s_info.csv' % (pwd, date)
print(file_source, file_target1, file_target2)

with open(file_source) as source, \
        open(file_target1, 'w') as target1, \
        open(file_target2, 'w') as target2:
    reader = csv.reader(source)
    writer_contact = csv.writer(target1)
    writer_info = csv.writer(target2)
    writer_contact.writerow(
        ['Account number', 'LID', 'Name', 'Number', 'Relationship', 'Remark']
    )
    for index, row in enumerate(reader):
        print(row)
        print(row[0], row[20].split('^'))

        user_id = row[0]
        lid = row[1]
        contacts = row[20].split('^')

        info = row[:-1]
        writer_info.writerow(info)

        if index == 0:
            continue
        for c in contacts:
            c = c.split('|')
            writer_contact.writerow([user_id, lid] + c)
