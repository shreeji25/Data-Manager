# migrate.py  — run once: python migrate.py
from database import engine
from sqlalchemy import text

def run():
    with engine.connect() as conn:

        print("Adding user_id to datasets...")
        conn.execute(text("""
            ALTER TABLE datasets 
            ADD COLUMN IF NOT EXISTS user_id INTEGER
        """))

        print("Adding category_id to datasets...")
        conn.execute(text("""
            ALTER TABLE datasets 
            ADD COLUMN IF NOT EXISTS category_id INTEGER
        """))

        print("Adding user_id to categories...")
        conn.execute(text("""
            ALTER TABLE categories 
            ADD COLUMN IF NOT EXISTS user_id INTEGER
        """))

        print("Removing old unique constraint on category name...")
        conn.execute(text("""
            ALTER TABLE categories 
            DROP CONSTRAINT IF EXISTS categories_name_key
        """))

        # ── Assign existing data to first admin user ──────────────────
        # Skip this block if your tables are empty
        result = conn.execute(text(
            "SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1"
        ))
        admin = result.fetchone()

        if admin:
            admin_id = admin[0]
            print(f"Assigning existing rows to admin user id={admin_id}...")
            conn.execute(text(
                f"UPDATE datasets SET user_id = {admin_id} WHERE user_id IS NULL"
            ))
            conn.execute(text(
                f"UPDATE categories SET user_id = {admin_id} WHERE user_id IS NULL"
            ))
        else:
            print("No admin found — skipping data assignment (tables must be empty)")

        print("Setting NOT NULL on user_id columns...")
        conn.execute(text("""
            ALTER TABLE datasets 
            ALTER COLUMN user_id SET NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE categories 
            ALTER COLUMN user_id SET NOT NULL
        """))

        print("Adding foreign keys...")
        conn.execute(text("""
            ALTER TABLE datasets 
            ADD CONSTRAINT IF NOT EXISTS fk_datasets_user 
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        """))
        conn.execute(text("""
            ALTER TABLE datasets 
            ADD CONSTRAINT IF NOT EXISTS fk_datasets_category 
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL
        """))
        conn.execute(text("""
            ALTER TABLE categories 
            ADD CONSTRAINT IF NOT EXISTS fk_categories_user 
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        """))

        print("Adding unique constraint per user on category name...")
        conn.execute(text("""
            ALTER TABLE categories 
            ADD CONSTRAINT IF NOT EXISTS uq_category_name_per_user 
            UNIQUE (name, user_id)
        """))

        print("Adding indexes...")
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_dataset_user_id ON datasets(user_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_category_user_id ON categories(user_id)
        """))

        conn.commit()
        print("\n✅ Migration complete.")

if __name__ == "__main__":
    run()