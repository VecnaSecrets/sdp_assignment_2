from EdInstitution import EdInstitution
from terminal import Terminal

ed = EdInstitution('lol', [], [])
ed.restoreFromFile('Innopolis.json')

term = Terminal([ed])
term.start()

ed.saveToFile('new.json')
