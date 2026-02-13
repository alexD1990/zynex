class Rule:
    name: str

    def apply(self, df, context=None):
        raise NotImplementedError
