import yaml
import glob
import os
import pandas as pd
from lxml import etree
import pyodbc

def load_config(path='config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def clean_and_parse(xml_file):
    with open(xml_file, 'rb') as f:
        content = f.read()
    start = content.find(b'<?xml')
    cleaned = content[start:] if start != -1 else content
    return etree.fromstring(cleaned)

def extract_common_fields(tree, common_paths, ns):
    def get(path):
        el = tree.find(path, namespaces=ns)
        return el.text.strip() if el is not None and el.text else 'NULL'
    return {k: get(v) for k, v in common_paths.items()}

def extract_data(tree, xpath_expr, ns, common_fields):
    elements = tree.xpath(xpath_expr, namespaces=ns)
    if not elements:
        print("[WARNING] No matching elements found.")
        return None

    data = []
    for elem in elements:
        row = {}
        for child in elem.iter():
            tag = child.tag.split('}')[-1]
            if child.text and child.text.strip():
                row[tag] = child.text.strip()
        if row:
            data.append(row)

    df = pd.DataFrame(data)
    for k, v in common_fields.items():
        df[k] = v
    return df

def save_to_db(df, table_name, db_config):
    try:
        conn = pyodbc.connect(
            Driver='{SQL Server}',
            Server=db_config['server'],
            Database=db_config['database'],
            Trusted_Connection='yes'
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?)
            BEGIN
                CREATE TABLE [{table_name}] ({','.join(f'[{col}] NVARCHAR(MAX)' for col in df.columns)})
            END
        """, (table_name,))

        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?", (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())
        for col in df.columns:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE [{table_name}] ADD [{col}] NVARCHAR(MAX)")

        for _, row in df.iterrows():
            placeholders = ','.join(['?'] * len(row))
            column_names = ','.join(f'[{col}]' for col in row.index)
            insert_sql = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(str(row[col]) if not pd.isna(row[col]) else None for col in row.index))

        conn.commit()
        print(f"[INFO] Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    config = load_config()

    ns = config['namespaces']
    db_config = config['database']
    input_dirs = config['general']['input_dirs']
    process_all = config['general']['process_all']

    for table_cfg in config['tables']:
        table_name = table_cfg['name']
        xpath_expr = table_cfg['xpath']
        common_paths = table_cfg['common_fields']

        for directory in input_dirs:
            files = glob.glob(os.path.join(directory, "*.xml"))
            for xml_file in files:
                print(f"\n[INFO] Processing file: {xml_file} for table: {table_name}")
                tree = clean_and_parse(xml_file)
                common_values = extract_common_fields(tree, common_paths, ns)
                df = extract_data(tree, xpath_expr, ns, common_values)
                if df is not None:
                    save_to_db(df, table_name, db_config)

if __name__ == "__main__":
    main()
