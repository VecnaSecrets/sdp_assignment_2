from room import Room


class Classroom(Room):
    def __str__(self):
        return 'Classroom ' + super().__str__()
