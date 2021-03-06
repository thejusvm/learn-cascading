

PAD_TEXT = "<pad>"
DEFAULT_CLICK_TEXT = "<defaultclick>"
MISSING_DATA_TEXT = "<missing-val>"

OUTPUTS_PER_ATTRIBUTE = ["positive", "negative", "clicked", "bought"]
DEFAULT_DICT_KEYS = [PAD_TEXT, MISSING_DATA_TEXT, DEFAULT_CLICK_TEXT]

# POSITIVE_COL_PREFIX = "positive"
# NEGATIVE_COL_PREFIX = "negative"
# CLICK_COL_PRERFIX = "clicked"
# BOUGHT_COL_PREFIX = "bought"

# COL_PREFIXES = tuple([POSITIVE_COL_PREFIX, NEGATIVE_COL_PREFIX, CLICK_COL_PRERFIX, BOUGHT_COL_PREFIX])
# TRAINING_COL_PREFIXES = tuple(["positive", "negative", "clicked", "bought"])
TRAINING_COL_PREFIXES = tuple(["positive", "negative", "negative_with_random", "uniform_random_negative", "impression_random_negative", "clicked_short", "clicked_long", "bought"])