import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split


def load_and_clean_data(file_path):
    """
    Load and clean the books dataset with robust error handling.

    Parameters:
    - file_path (str): Path to the CSV file containing the books data.

    Returns:
    - DataFrame: Cleaned pandas DataFrame containing the books data.
    """
    try:
        # Read CSV with more flexible parsing options
        df = pd.read_csv(
            file_path,
            error_bad_lines=False,
            encoding='utf-8',
            low_memory=False,  # Avoid dtype guessing issues
            escapechar='\\',  # Handle escaped characters
            quoting=1  # Handle quoted fields (QUOTE_ALL)
        )

        # Clean text fields
        text_columns = ['title', 'authors']
        for col in text_columns:
            if col in df.columns:
                # Replace missing values with empty string
                df[col] = df[col].fillna('')
                # Clean whitespace
                df[col] = df[col].str.strip()

        # Create combined text features with error handling
        df['text_features'] = df.apply(
            lambda x: f"{x['title']} {x['authors']}"
            if 'title' in df.columns and 'authors' in df.columns
            else "", axis=1
        )

        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise


def prepare_features(df, numerical_features):
    """
    Prepare text and numerical features with error handling.

    Parameters:
    - df (DataFrame): pandas DataFrame containing the books data.
    - numerical_features (list): List of numerical feature column names.

    Returns:
    - tuple: Combined feature matrix (numpy array), fitted TF-IDF vectorizer, and fitted scaler.
    """
    # Text features
    tfidf = TfidfVectorizer(
        stop_words="english",
        max_features=5000,  # Limit features to prevent memory issues
        strip_accents='unicode',
        token_pattern=r'\w+'
    )

    # Handle potential empty text features
    text_features = df["text_features"].fillna('')
    tfidf_matrix = tfidf.fit_transform(text_features)

    # Numerical features
    scaler = StandardScaler()
    # Fill missing values with median
    numerical_data = df[numerical_features].fillna(df[numerical_features].median())
    numerical_matrix = scaler.fit_transform(numerical_data)

    # Combine features by horizontally stacking TF-IDF and numerical features
    combined_features = np.hstack((tfidf_matrix.toarray(), numerical_matrix))

    return combined_features, tfidf, scaler


def calculate_map_k(similarities, k=10, similarity_threshold=0.5):
    """
    Calculate Mean Average Precision at K (MAP@K) with error handling.

    Parameters:
    - similarities (numpy array): Array of similarity scores between validation and training samples.
    - k (int): Number of top recommendations to consider.
    - similarity_threshold (float): Threshold to determine relevance.

    Returns:
    - float: MAP@K score.
    """
    try:
        precisions = []
        for i in range(len(similarities)):
            # Get indices of top K similar items
            top_k = np.argsort(similarities[i])[-k:][::-1]
            # Determine which of the top K items are relevant based on the similarity threshold
            relevant = similarities[i][top_k] > similarity_threshold
            if relevant.sum() > 0:
                # Calculate precision at each relevant position
                precision_at_k = np.cumsum(relevant) / np.arange(1, k + 1)
                # Average precision for this instance
                ap = np.sum(precision_at_k * relevant) / min(k, relevant.sum())
                precisions.append(ap)
        # Return the mean of average precisions
        return np.mean(precisions) if precisions else 0.0
    except Exception as e:
        print(f"Error calculating MAP@K: {str(e)}")
        return 0.0


def main():
    try:
        # ---------------------------------------------------
        # Configuration
        # ---------------------------------------------------
        file_path = "books_cleaned.csv"
        numerical_features = ["average_rating", "ratings_count", "text_reviews_count"]

        # ---------------------------------------------------
        # Step 1: Load and Preprocess Data
        # ---------------------------------------------------

        # Load and clean the dataset
        df = load_and_clean_data(file_path)

        # ---------------------------------------------------
        # Step 2: Split Data into Training and Validation Sets
        # ---------------------------------------------------

        # Split the data into training and validation sets with an 80-20 split
        train_df, val_df = train_test_split(df, test_size=0.2, random_state=42)

        # ---------------------------------------------------
        # Step 3: Prepare Features for Training
        # ---------------------------------------------------

        # Prepare features and obtain fitted TF-IDF vectorizer and scaler
        combined_features, tfidf, scaler = prepare_features(train_df, numerical_features)

        # ---------------------------------------------------
        # Step 4: Prepare Features for Validation Set
        # ---------------------------------------------------

        # Transform the validation set using the fitted TF-IDF vectorizer
        val_tfidf = tfidf.transform(val_df["text_features"].fillna(''))
        # Transform the numerical features of the validation set using the fitted scaler
        val_numerical = scaler.transform(val_df[numerical_features].fillna(val_df[numerical_features].median()))
        # Combine TF-IDF and numerical features for the validation set
        val_combined = np.hstack((val_tfidf.toarray(), val_numerical))

        # ---------------------------------------------------
        # Step 5: Calculate Cosine Similarities
        # ---------------------------------------------------

        # Calculate cosine similarity between validation set and training set features
        similarities = cosine_similarity(val_combined, combined_features)

        # ---------------------------------------------------
        # Step 6: Calculate and Print MAP@10
        # ---------------------------------------------------

        # Calculate Mean Average Precision at K=10
        map_10 = calculate_map_k(similarities, k=10)
        print(f"MAP@10: {map_10:.4f}")

        # ---------------------------------------------------
        # Step 7: Save Sample Recommendations
        # ---------------------------------------------------

        # Generate and save sample recommendations for the first 5 validation samples
        sample_recommendations = pd.DataFrame({
            "book_id": val_df["bookID"],
            "recommended_books": [
                " ".join(train_df["bookID"].iloc[np.argsort(similarities[i])[-5:][::-1]].astype(str))
                for i in range(len(val_df))
            ]
        })
        sample_recommendations.to_csv("submission.csv", index=False)

    except Exception as e:
        print(f"Error in main execution: {str(e)}")


if __name__ == "__main__":
    main()