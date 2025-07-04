import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

DB_PATH = "auctions.db"

app = Flask(__name__)
CORS(app)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/auctions', methods=['GET'])
def get_auctions():
    # Optional: sort_by (e.g., "id" or any flat column), order ("asc" or "desc"), limit
    sort_by = request.args.get('sort_by', None)
    order = request.args.get('order', 'asc').lower()
    limit = request.args.get('limit', 100)
    try:
        limit = int(limit)
    except ValueError:
        limit = 100

    conn = get_db_connection()
    c = conn.cursor()

    # Get the total number of entries
    c.execute("SELECT COUNT(*) FROM auctions")
    total_count = c.fetchone()[0]

    # Get valid columns for validation
    c.execute("PRAGMA table_info(auctions)")
    columns = {row['name'] for row in c.fetchall()}

    base_query = "SELECT * FROM auctions"

    # Validate and use flat columns for sorting
    if sort_by and sort_by in columns:
        base_query += f' ORDER BY "{sort_by}" {order.upper()}'
    else:
        base_query += f" ORDER BY timestamp {order.upper()}"

    base_query += f" LIMIT ?"
    rows = c.execute(base_query, (limit,)).fetchall()
    conn.close()

    # Return the count and the data
    return jsonify({
        "total_count": total_count,
        "auctions": [{k: v for k, v in dict(row).items() if v is not None} for row in rows]
    })

@app.route('/auction/<auction_id>', methods=['GET'])
def get_auction(auction_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM auctions WHERE auction_id = ?", (auction_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return jsonify({k: v for k, v in dict(row).items() if v is not None})
    return jsonify({"error": "Not found"}), 404

@app.route('/auctions/by_id/<item_id>', methods=['GET'])
def get_auctions_by_item_id(item_id):
    limit = request.args.get('limit', 100)
    try:
        limit = int(limit)
    except ValueError:
        limit = 100
    conn = get_db_connection()
    c = conn.cursor()
    # Try both "item_id" and "id" as column, fallback to "item_id"
    query = """
        SELECT * FROM auctions
        WHERE id = ?
        LIMIT ?
    """
    rows = c.execute(query, (item_id, limit)).fetchall()
    conn.close()
    return jsonify([{k: v for k, v in dict(row).items() if v is not None} for row in rows])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # Accessible on local network