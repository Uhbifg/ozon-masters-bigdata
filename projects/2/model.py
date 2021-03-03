numeric_features = ["if"+str(i) for i in range(1,14)]
#categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]

fields = ["id", "label"] + numeric_features

model = Pipeline(steps=[
    ('linearregression', LinearRegression())
])