class Rule:
    name: str

    def apply(self, df):
        raise NotImplementedError
