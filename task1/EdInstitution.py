import json
from Classroom import Classroom
from LectureAuditorium import LectureAuditorium
from activity import Activity
from datetime import datetime, date


class EdInstitution:

    @staticmethod
    def json_default(value):
        """Static method that used to properly encode class to JSON

        :param value: attribute of class to be encoded
        """
        if isinstance(value, date):
            return value.strftime('%d-%m-%Y, %H:%M:%S')
        else:
            return value.__dict__

    def __init__(self, name, classrooms, auditoriums):
        self.name = name
        self.classrooms = classrooms
        self.LectureAuditoriums = auditoriums

    # Setters
    def set_name(self, name):
        self.name = name

    def set_classrooms(self, classrooms):
        self.classrooms = classrooms

    def set_auditoriums(self, auditoriums):
        self.LectureAuditoriums = auditoriums

    # Getters
    def get_name(self):
        return self.name

    def get_classrooms(self):
        return self.classrooms

    def get_auditoriums(self):
        return self.LectureAuditoriums

    def __str__(self):
        free_classrooms = self.find_free_rooms(self.classrooms)
        free_auditoriums = self.find_free_rooms(self.LectureAuditoriums)
        return f'{self.name}\n' \
               f'Classroom(s):{len(self.classrooms)}\n' \
               f'Auditorium(s){len(self.LectureAuditoriums)}\n' \
               f'Available:\tClassrooms:{free_classrooms}\n' \
               f'\t\t\tLecture auditoriums:{free_auditoriums}\n'\
               f'(Current time:{datetime.now().time().strftime("%H:%M:%S")})\n'

    def restoreFromFile(self, filename):
        f = open(filename)
        data = json.load(f)
        f.close()

        self.name = data['name']

        for classroom in data['classrooms']:
            new_classroom = Classroom(classroom['capacity'], classroom['number'], classroom['has_conditioner'])
            for activity in classroom['activities']:
                start = datetime.strptime(activity['start'], '%d-%m-%Y, %H:%M:%S')
                end = datetime.strptime(activity['end'], '%d-%m-%Y, %H:%M:%S')
                new_classroom.add_activity(Activity(activity['name'], start, end))
            self.classrooms.append(new_classroom)

        for auditorium in data['LectureAuditoriums']:
            new_auditorium = LectureAuditorium(auditorium['capacity'], auditorium['number'], auditorium['has_conditioner'])
            for activity in auditorium['activities']:
                start = datetime.strptime(activity['start'], '%d-%m-%Y, %H:%M:%S')
                end = datetime.strptime(activity['end'], '%d-%m-%Y, %H:%M:%S')
                new_auditorium.add_activity(Activity(activity['name'], start, end))
            self.LectureAuditoriums.append(new_auditorium)

    def saveToFile(self, filename):
        f = open(filename, 'w')
        f.write(json.dumps(self, default=self.json_default, indent=4))
        f.close()

    def find_room(self, number):
        for room in self.classrooms + self.LectureAuditoriums:
            if room.number == number:
                return room

    def add_room(self, is_classroom, room):
        if is_classroom:
            self.classrooms.append(room)
        else:
            self.LectureAuditoriums(room)

    def remove_room(self, is_classroom, number):
        if is_classroom:
            self.classrooms = list(filter(lambda x: x.number != number, self.classrooms))
        else:
            self.LectureAuditoriums = list(filter(lambda x: x.number != number, self.LectureAuditoriums))

    def print_classrooms(self):
        for classroom in self.classrooms:
            print(classroom)

    def print_auditoriums(self):
        for auditorium in self.LectureAuditoriums:
            print(auditorium)

    def print_all_rooms(self):
        self.print_classrooms()
        self.print_auditoriums()

    @staticmethod
    def find_free_rooms(rooms):
        return list(filter(lambda x: x.is_free(), rooms))
