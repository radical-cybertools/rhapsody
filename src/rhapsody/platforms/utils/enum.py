
class EnumTypes:

    def __init__(self, *args):
        """Initialization.

        Args:
            args (tuple): Pairs of ('Name', 'value').
        """
        self.values = [x[1] for x in args]
        self.choices = list(zip(self.values, [x[0] for x in args]))

        self._attrs = dict(args)
        self._items = dict(zip(range(len(args)), self.values))

    def __getattr__(self, a):
        """Get value of the corresponding Enum name.

        Args:
            a (str): Enum name.

        Returns:
            any: Enum value.
        """
        return self._attrs[a]

    def __getitem__(self, i):
        """Get value of the corresponding element by the index.

        Args:
            i (str): Enum index.

        Returns:
            any: Enum value.
        """
        return self._items[i]

    def __len__(self):
        """Get number of elements.

        Returns:
            int: Number of elements.
        """
        return len(self._attrs)

    def __iter__(self):
        return iter(self.choices)

