import sqlite3
conn = sqlite3.connect('auctions.db')
c = conn.cursor()
print(list(c.execute('SELECT COUNT(*) FROM auctions')))
print(list(c.execute('SELECT * FROM auctions LIMIT 1')))
conn.close()