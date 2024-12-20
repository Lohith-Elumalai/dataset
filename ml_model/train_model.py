import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from joblib import dump

# Load data from processed logs
data_path = "./processed_logs.csv"

def train_model():
    # Load dataset
    data = pd.read_csv(data_path)

    # Assume the dataset contains 'log_message' and 'label' columns
    X = data['log_message']
    y = data['label']  # Labels: e.g., "error", "warning", "info"

    # Convert text data to numeric (simple example with TF-IDF)
    from sklearn.feature_extraction.text import TfidfVectorizer
    vectorizer = TfidfVectorizer(max_features=1000)
    X_tfidf = vectorizer.fit_transform(X)

    # Split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X_tfidf, y, test_size=0.2, random_state=42)

    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate accuracy
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model Training Complete. Accuracy: {accuracy * 100:.2f}%")

    # Save the model and vectorizer
    dump(model, "./ml_model/model.pkl")
    dump(vectorizer, "./ml_model/vectorizer.pkl")

if __name__ == "__main__":
    train_model()
