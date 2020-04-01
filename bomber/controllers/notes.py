from bottle import get
from bomber.models import Notes


@get('/api/v1/get_notes')
def get_notes():
    notes = Notes.select()
    result = {}
    for note in notes:
        if result.get(note.groups):
            result[note.groups].append(note.note)
        else:
            result[note.groups] = [note.note]
    data = []
    for key in result.keys():
        data.append({'label': key, 'value': result[key]})
    return data
