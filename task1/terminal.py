from Classroom import Classroom
from LectureAuditorium import LectureAuditorium
from datetime import datetime
from activity import Activity


class Terminal:
    def __init__(self, institutions):
        self.EdInstitutions = institutions

    def start(self):
        action = 0
        while action != 5:
            self.print_actions()
            action = int(input('Choose an action:'))
            self.do_action(action)
        for inst in self.EdInstitutions:
            print(inst)

    @staticmethod
    def print_actions():
        print("\t1 : Add classroom or Auditorium to institution\n"
              "\t2 : Print institution summary\n"
              "\t3 : Assign activity\n"
              "\t4 : Print room summary\n"
              "\t5 : Exit program\n")

    @staticmethod
    def get_room_type():
        print('Enter room type:\n1. Classroom\n 2. Lecture auditorium')
        return int(input()) == 1

    @staticmethod
    def get_room_props(room_type):
        print('Enter room properties:\n')
        capacity = int(input('Capacity: '))
        number = int(input('Number: '))
        conditioner = input('Has air conditioner?(yes or no) ')
        if room_type:
            return Classroom(capacity, number, conditioner == 'yes')
        else:
            return LectureAuditorium(capacity, number, conditioner == 'yes')

    def get_room_number(self, inst_ind, room_type):
        if room_type:
            rooms = self.EdInstitutions[inst_ind].classrooms
        else:
            rooms = self.EdInstitutions[inst_ind].LectureAuditoriums
        print('Choose rome number from list below:\n')
        for ind, room in enumerate(rooms):
            print(f'{ind + 1}. {room.number}')
        return int(input()) - 1

    def get_institution(self):
        print('Enter institution number from list below:\n')
        for ind, inst in enumerate(self.EdInstitutions):
            print(f'{ind + 1}. {inst.name}')
        return int(input()) - 1

    @staticmethod
    def get_activity():
        print('Enter activity name: ')
        name = input()
        print('Enter start time(format: HH:MM::SS):')
        start = datetime.strptime(input(), '%H:%M:%S')
        print('Enter end time(format: HH:MM::SS):')
        end = datetime.strptime(input(), '%H:%M:%S')
        return Activity(name, start, end)

    def add_institution(self, institution):
        self.EdInstitutions.append(institution)

    def do_action(self, action):
        if action == 1:
            self.add_pipeline()
        elif action == 2:
            self.print_inst_pipeline()
        elif action == 3:
            self.activity_assigning_pipeline()
        elif action == 4:
            self.print_room_pipeline()

    def add_pipeline(self):
        inst_ind = self.get_institution()
        room_type = self.get_room_type()
        room = self.get_room_props(room_type)
        self.EdInstitutions[inst_ind].add_room(room_type, room)

    def print_inst_pipeline(self):
        inst_ind = self.get_institution()
        print(self.EdInstitutions[inst_ind])

    def activity_assigning_pipeline(self):
        inst_ind = self.get_institution()
        room_type = self.get_room_type()
        room_ind = self.get_room_number(inst_ind, room_type)
        activity = self.get_activity()

        if room_type:
            self.EdInstitutions[inst_ind].classrooms[room_ind].add_activity(activity)
        else:
            self.EdInstitutions[inst_ind].LectureAuditoriums[room_ind].add_activity(activity)

    def print_room_pipeline(self):
        inst_ind = self.get_institution()
        room_type = self.get_room_type()
        room_ind = self.get_room_number(inst_ind, room_type)
        if room_type:
            print(self.EdInstitutions[inst_ind].classrooms[room_ind])
        else:
            print(self.EdInstitutions[inst_ind].LectureAuditoriums[room_ind])
