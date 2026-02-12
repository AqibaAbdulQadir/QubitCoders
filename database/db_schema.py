import sqlite3
import requests
import csv
from fetch_data import save_rankings

conn = sqlite3.connect("students.db")
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS STUDENT_INFO
                (username TEXT PRIMARY KEY,
                full_name TEXT,
                batch TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS PROBLEMS_SOLVED
                (username TEXT PRIMARY KEY,
                problems_solved INTEGER,
                easy INTEGER,
                medium INTEGER,
                hard INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS CONTEST_INFO
                (username TEXT PRIMARY KEY,
                contest_count INTEGER,
                contest_rating FLOAT)''')


# Uncomment to insert/update new user data in STUDENT_INFO Table
with open('raw_data.csv', 'r') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)

    for row in csv_reader:
        cur.execute('INSERT OR REPLACE INTO STUDENT_INFO (full_name, username, batch) VALUES (?, ?, ?)', row)
conn.commit()


# Uncomment to insert/update new contest/submission data in the CONTEST_INFO and PROBLEMS_SOLVED table
cur.execute('SELECT username FROM STUDENT_INFO')
rows = cur.fetchall()

base = "https://leetcode-api-pied.vercel.app"

for row in rows:
    username = row[0]
    try:
        r = requests.get(f'{base}/user/{username}/contests')
        data = r.json()
        contest_data = (username, data['userContestRanking']['attendedContestsCount'],
                        data['userContestRanking']['rating'])
        cur.execute('INSERT OR REPLACE INTO CONTEST_INFO (username, contest_count, contest_rating) '
                    'VALUES (?, ?, ?)', contest_data)

    except Exception as e:
        # print(username, e, data)
        cur.execute('INSERT OR REPLACE INTO CONTEST_INFO (username, contest_count, contest_rating) '
                    'VALUES (?, ?, ?)', (username, 0, 0))


    try:
        r = requests.get(f'{base}/user/{username}')
        data = r.json()
        subs = data["submitStats"]["acSubmissionNum"]
        submission_data = (username, subs[0]["count"], subs[1]["count"], subs[2]["count"], subs[3]["count"])
        cur.execute('INSERT OR REPLACE INTO PROBLEMS_SOLVED (username, problems_solved, easy, medium, hard) '
                    'VALUES (?, ?, ?, ?, ?)', submission_data)
    except Exception as e:
        # print(username, e, data)
        cur.execute('INSERT OR REPLACE INTO PROBLEMS_SOLVED (username, problems_solved, easy, medium, hard) '
                    'VALUES (?, ?, ?, ?, ?)', (username, 0, 0, 0, 0))
conn.commit()

# Uncomment to test
cur.execute('SELECT * FROM student_info')
rows1 = cur.fetchall()
cur.execute('SELECT * FROM problems_solved')
rows2 = cur.fetchall()
print(rows1, rows2)
conn.close()

# Uncomment to save new data to ranking_table.csv
save_rankings()


