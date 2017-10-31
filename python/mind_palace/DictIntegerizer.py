

class DictIntegerizer :

    def __init__(self, name= "dict", default = None) :
        self.name = name
        self.termdict = {}
        self.currentCount = 0
        self.default = default
        if(default != None) :
            for item in default:
                self.get(item)

    def _add_(self, term) :
        self.termdict[term] = self.currentCount
        self.currentCount = self.currentCount + 1

    def getdefault(self):
        return self.default

    def getdefaultindex(self):
        if self.default is not None :
            return self.get(self.default)
        else :
            return -1

    def get(self, term) :
        term = term.lower()
        if term not in self.termdict :
            self._add_(term)
        return self.termdict[term]

    def only_get(self, term, missing_val = -1):
        term = term.lower()
        if term not in self.termdict :
            return missing_val
        return self.termdict[term]

    def getDict(self) :
        return self.termdict

    def dictSize(self) :
        return len(self.termdict)

    def __eq__(self, other):
        return self.termdict == other.termdict and self.currentCount == other.currentCount

    def __str__(self):
        return self.name + ":" + str(self.currentCount)
