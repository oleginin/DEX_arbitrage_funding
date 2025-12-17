import argparse

parser = argparse.ArgumentParser(description="This program prints recipes \
consisting of the ingredients you provide.")
parser.add_argument("-i1", "--ingredient_1")  # optional argument
                                              # or
parser.add_argument("ingredient_1")
parser.add_argument("-i2", "--ingredient_2",
                    choices=["pasta", "rice", "potato", "onion",
                             "garlic", "carrot", "soy_sauce", "tomato_sauce"],
                    help="You need to choose only one ingredient from the list.")
# positional argument

print(parser)