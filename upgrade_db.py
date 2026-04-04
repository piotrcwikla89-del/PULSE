import sqlite3
import os
import sys

# Dynamiczna ścieżka bazy danych
def get_db_path():
    if getattr(sys, 'frozen', False):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(__file__)
    return os.path.join(base, "database.db")

DB = get_db_path()
conn = sqlite3.connect(DB)
cur = conn.cursor()

# Dodanie tabeli users
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL,   -- 'admin' lub 'operator'
    password TEXT         -- tylko dla admina, dla operatora NULL
)
""")

# Sprawdź, czy admin już istnieje (żeby nie duplikować)
cur.execute("SELECT * FROM users WHERE username='admin'")
if not cur.fetchone():
    # Dla uproszczenia hasło w plain text – w realnej aplikacji powinno być hashowane (np. bcrypt)
    # Tu dajemy przykładowe hasło "admin123". Możesz zmienić na swoje.
    cur.execute("INSERT INTO users (username, role, password) VALUES (?, ?, ?)",
                ("admin", "admin", "admin123"))

# Dodaj przykładowego użytkownika drukarza
cur.execute("SELECT * FROM users WHERE username='operator1'")
user = cur.fetchone()
if user:
    # Zaktualizuj istniejącego operatora na drukarza i przypisz hasło
    cur.execute("UPDATE users SET role='drukarz', password='drukarz123' WHERE username='operator1'")
else:
    # Dodaj nowego drukarza jeśli nie istnieje
    cur.execute("INSERT INTO users (username, role, password) VALUES (?, ?, ?)",
                ("drukarz1", "drukarz", "drukarz123"))

conn.commit()
conn.close()

print("✅ Baza danych zaktualizowana – dodano tabelę users i przykładowych użytkowników.")
print("   Admin: admin / admin123")
print("   Drukarz: drukarz1 / drukarz123 (lub operator1 / drukarz123)")