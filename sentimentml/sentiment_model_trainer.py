# Author: WEE LING HUE

from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sentimentml.text_preprocessor import text_preprocessor  # Import text_preprocessor

class sentiment_model_trainer:
    def __init__(self, label_col="label", feature_col="features"):
        self.label_col = label_col
        self.feature_col = feature_col
        self.models = {
            "naive_bayes": NaiveBayes(labelCol=label_col, featuresCol=feature_col),
            "random_forest": RandomForestClassifier(labelCol=label_col, featuresCol=feature_col),
            "logistic_regression": LogisticRegression(labelCol=label_col, featuresCol=feature_col, family="multinomial"),
            "decision_tree": DecisionTreeClassifier(labelCol=label_col, featuresCol=feature_col)
        }
        
        self.param_grids = {
            "naive_bayes": ParamGridBuilder().addGrid(self.models["naive_bayes"].smoothing, [10.0, 20.0, 30.0, 40.0, 50.0]).build(),  
            "random_forest": ParamGridBuilder().addGrid(self.models["random_forest"].numTrees, [10, 20, 30, 50, 100]).addGrid(self.models["random_forest"].maxDepth, [3, 4, 5]).addGrid(self.models["random_forest"].minInstancesPerNode, [1, 2, 3, 5, 7]).addGrid(self.models["random_forest"].minInfoGain, [0.01, 0.05, 0.1]).addGrid(self.models["random_forest"].subsamplingRate, [0.6, 0.7, 0.8]).build(),
            "logistic_regression": ParamGridBuilder().addGrid(self.models["logistic_regression"].maxIter, [10, 20, 30]).addGrid(self.models["logistic_regression"].regParam, [0.001, 0.01, 0.1, 0.5, 1.0]).addGrid(self.models["logistic_regression"].elasticNetParam, [0.0, 0.5, 1.0]).build(),
            "decision_tree": ParamGridBuilder().addGrid(self.models["decision_tree"].maxDepth, [5, 10, 15, 20]).addGrid(self.models["decision_tree"].minInstancesPerNode, [1, 2, 5]).build()

        }
        
        self.evaluators = {
            "accuracy": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="accuracy"),
            "precision": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedPrecision"),
            "recall": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedRecall"),
            "f1": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="f1")
        }
    
    def train(self, train_data, model_type):
        
        pipeline = Pipeline(stages=[self.models[model_type]])
        
        # Cross-validation
        crossval = CrossValidator(
            estimator=pipeline,  
            estimatorParamMaps=self.param_grids[model_type],
            evaluator=self.evaluators["accuracy"],
            numFolds=5
        )
        
        # Train model
        pipeline_model = crossval.fit(train_data)
        return pipeline_model

    def evaluate(self, model, train_data, test_data):
        # Evaluate trained model on both training and testing data to check for overfitting.
        
        # Get predictions for both train and test datasets
        train_predictions = model.transform(train_data)
        test_predictions = model.transform(test_data)
    
        # Evaluate train and test performance
        train_results = {metric: self.evaluators[metric].evaluate(train_predictions) for metric in self.evaluators}
        test_results = {metric: self.evaluators[metric].evaluate(test_predictions) for metric in self.evaluators}
    
        # Print results
        print(" Training Set Performance: ")
        for metric, value in train_results.items():
            print(f"  {metric.capitalize()}: {value:.4f}")
    
        print("\n Testing Set Performance: ")
        for metric, value in test_results.items():
            print(f"  {metric.capitalize()}: {value:.4f}")
    
        # Check for overfitting (If difference > 1 is overfit)
        accuracy_gap = train_results["accuracy"] - test_results["accuracy"]
        
        if accuracy_gap > 0.1:  # Adjust threshold based on need
            print("\n!Possible Overfitting Detected! ")
            print(f"Training accuracy is {train_results['accuracy']:.4f}, but testing accuracy is {test_results['accuracy']:.4f}.")
        else:
            print("\n No significant overfitting detected. Model generalizes well! ")
    
        return train_results, test_results

