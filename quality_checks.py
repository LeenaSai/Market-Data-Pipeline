def main():
    df = fetch_data()
    df_clean = clean_data(df)
    insert_clean_data(df_clean)
    print(f"Cleaned data inserted with {len(df_clean)} records.")

if __name__ == "__main__":
    main()
