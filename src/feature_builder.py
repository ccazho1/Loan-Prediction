class FeatureBuilder:
    def __init__(self):
        self.steps = []

    def register(self, func):
        self.steps.append(func)
        return func

    def run(self, df):
        for step in self.steps:
            df = step(df)
        return df

fb = FeatureBuilder()
