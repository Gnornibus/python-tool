import os
import json
import mysql.connector
from opencc import OpenCC

def create_tables(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS poems (
                id VARCHAR(36) PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                author VARCHAR(255) NOT NULL,
                content TEXT NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS poem_tags (
                poem_id VARCHAR(36) NOT NULL,
                tag_id INT NOT NULL,
                PRIMARY KEY (poem_id, tag_id),
                FOREIGN KEY (poem_id) REFERENCES poems(id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
            );
        """)
    except mysql.connector.Error as err:
        print(f"SQL Error: {err.msg}")
        if err.errno != 1050:  # If not 'table already exists', raise error.
            raise

def load_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    cc = OpenCC('t2s')  # Simplify characters
    for poem in data:
        poem['title'] = cc.convert(poem['title'])
        poem['author'] = cc.convert(poem['author'])
        poem['paragraphs'] = [cc.convert(paragraph) for paragraph in poem['paragraphs']]
        poem['tags'] = [cc.convert(tag) for tag in poem.get('tags', [])]
    return data

def insert_data(data, cursor):
    for poem in data:
        content = '\n'.join(poem['paragraphs'])
        cursor.execute('''
            INSERT INTO poems (id, title, author, content) VALUES (%s, %s, %s, %s) AS new_poem
            ON DUPLICATE KEY UPDATE title=new_poem.title, author=new_poem.author, content=new_poem.content;
        ''', (poem['id'], poem['title'], poem['author'], content))

        for tag in poem['tags']:
            try:
                cursor.execute('INSERT INTO tags (name) VALUES (%s)', (tag,))
            except mysql.connector.Error as e:
                if e.errno != 1062:  # Ignore duplicate tag entries
                    raise
            cursor.execute('SELECT id FROM tags WHERE name = %s', (tag,))
            result = cursor.fetchone()
            tag_id = result[0] if result else None
            if tag_id:
                cursor.execute('''
                    INSERT INTO poem_tags (poem_id, tag_id) VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE poem_id=poem_id, tag_id=tag_id;
                ''', (poem['id'], tag_id))

def main():
    directory = r'E:\Code\chinese-poetry\全唐诗'
    exclude_files = {'authors.song.json', 'authors.tang.json','README.md','表面结构字.json'}  # Excluded files

    try:
        config = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'database': 'poetry',
            'raise_on_warnings': True
        }
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        create_tables(cursor)

        for filename in os.listdir(directory):
            if filename not in exclude_files and os.path.isfile(os.path.join(directory, filename)):
                print(f"Processing {filename}")
                poems_data = load_data(os.path.join(directory, filename))
                insert_data(poems_data, cursor)
                conn.commit()
            else:
                print(f"Skipping {filename}")

    except mysql.connector.Error as e:
        print(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
