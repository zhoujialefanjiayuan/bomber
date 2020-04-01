import pytest
from bomber.utils import sort_contact_by_relationship


class Contact:
    def __init__(self, relationship):
        self.relationship = relationship


def test_sort_contact_by_relationship():
    a = Contact(1)
    b = Contact(0)
    contacts = [a, b]
    actual = sort_contact_by_relationship(contacts)
    expect = [b, a]
    assert expect == actual
