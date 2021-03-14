from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from catboost import CatBoostRegressor


numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]

fields = ["id", "label"] + numeric_features + categorical_features


model_features = numeric_features

train_features = ["id"] + numeric_features + categorical_features

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features)
    ]
)


model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('linearregression', CatBoostRegressor(learning_rate=0.01, early_stopping_rounds=1000, iterations=1))
])

