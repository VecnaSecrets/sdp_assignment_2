from room import Room


class LectureAuditorium(Room):
    def __str__(self):
        return 'Lecture auditorium ' + super().__str__()
