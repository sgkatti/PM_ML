def safe_get(df, col):
    return df[col] if col in df else None


