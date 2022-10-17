from datetime import datetime, time


# Parent class for both - classrooms and lecture auditoriums
class Room:

    @staticmethod
    def is_time_between(begin_time, end_time, check_time=None):
        """
        Utility function checking if time is in interval (begin_time, end_time)
        """
        check_time = check_time or datetime.now().time()
        if begin_time < end_time:
            return begin_time <= check_time <= end_time
        else:
            return check_time >= begin_time or check_time <= end_time

    # Constructor
    def __init__(self, capacity, number, has_conditioner):
        self.capacity = capacity
        self.number = number
        self.has_conditioner = has_conditioner
        self.activities = []

    # Getters
    def get_number(self):
        return self.number

    def get_capacity(self):
        return self.capacity

    def is_has_conditioner(self):
        return self.has_conditioner

    # Setters
    def set_number(self, number):
        self.number = number

    def set_capacity(self, capacity):
        self.capacity = capacity

    def set_conditioner(self, has_conditioner):
        self.has_conditioner = has_conditioner

    def __str__(self):
        out = f'number {self.number}\n\t' \
              f'capacity:{self.capacity}\n\t' \
              f'has_conditioner:{self.has_conditioner}\n\t' \
              f'Activities:'
        for activity in self.activities:
            start = datetime.strftime(activity.start, '%H:%M:%S')
            end = datetime.strftime(activity.end, '%H:%M:%S')
            out += f'\t{activity.name} ' \
                   f'({start} - {end})\n\t\t\t'
        return out

    def __repr__(self):
        return str(self.number)

    def check_time(self, start, end):
        """
        Method checking if time is free and in working hours.

        :param start: activity start time
        :param end: activity end time
        :return: True if time interval is free, else - False
        """
        if start > end:
            print('time is out of working hours')
            return False
        if not self.is_time_between(time(8, 0), time(21, 0), start.time()) and not self.is_time_between(time(8, 0), time(21, 0), end.time()):
            return False
        for act in self.activities:
            if start < act.end and act.start < end:
                return False
        return True

    def add_activity(self, act):
        if self.check_time(act.start, act.end):     # Also Check if time in between 8:00 and 21:00
            self.activities.append(act)

    def is_free(self):
        for activity in self.activities:
            if self.is_time_between(activity.start.time(), activity.end.time()):
                return False
        return True
