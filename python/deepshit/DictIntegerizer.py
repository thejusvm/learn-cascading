

class DictIntegerizer :

    def __init__(self) :
        self.termdict = {}
        self.currentCount = 0

    def _add_(self, term) :
        self.termdict[term] = self.currentCount
        self.currentCount = self.currentCount + 1

    def get(self, term) :
        if term not in self.termdict :
            self._add_(term)
        return self.termdict[term]

    def getDict(self) :
        return self.termdict;

    def dictSize(self) :
        return len(self.termdict);

    def __eq__(self, other):
        return self.termdict == other.termdict and self.currentCount == other.currentCount
