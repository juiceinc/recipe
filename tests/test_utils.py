# -*- coding: utf-8 -*-
from datetime import date

import pytest
from faker import Faker
from faker.providers import BaseProvider

from recipe.compat import basestring
from recipe.utils import (
    AttrDict,
    FakerAnonymizer,
    FakerFormatter,
    replace_whitespace_with_space,
    generate_faker_seed,
)
from recipe.compat import str as compat_str

uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


class TestUtils(object):
    def test_replace_whitespace_with_space(self):
        assert replace_whitespace_with_space("fooo    moo") == "fooo moo"
        assert replace_whitespace_with_space("fooo\n\t moo") == "fooo moo"

    def test_generate_faker_seed(self):
        """ Test we get the same seed values in py2 and py3 """

        assert compat_str('2') == compat_str(u'2')
        assert generate_faker_seed(None) == 15208487303490345319
        assert generate_faker_seed(0) == 17387988563394623128
        assert generate_faker_seed(u'hi') == 14742342832905345683
        assert generate_faker_seed('hi') == 14742342832905345683
        assert generate_faker_seed([]) == 2766641713143040348


class TestAttrDict(object):
    def test_attr_dict(self):
        d = AttrDict()
        assert isinstance(d, dict)
        d.foo = 2
        assert d["foo"] == 2
        d["bar"] = 3
        assert d.bar == 3


class TestFakerFormatter(object):
    def test_formatter(self):
        formatter = FakerFormatter()
        # Faker providers can be accessed by attribute if they take no
        # arguments
        assert len(formatter.format("{fake:name}", fake=Faker())) > 0

        # They can no longer be accessed as callables
        with pytest.raises(AttributeError):
            formatter.format("{fake:name()}", fake=Faker())

        # Some attributes can't be found
        with pytest.raises(AttributeError):
            formatter.format("{fake:nm}", fake=Faker())

        # Parameterized values still work
        assert len(formatter.format("{fake:numerify|text=###}", fake=Faker())) == 3


class CowProvider(BaseProvider):
    def moo(self):
        return "moo"



DEPARTMENT_CHOICES = ['Surgery', 'Radiology', 'Neonatal', 'Cardiology',
                      'Pediatrics', 'General', 'Emergency', 'Orthopedic',
                      'Audiology', 'Dermatology', 'Endocrinology',
                      'Gynecology', 'Hematology', 'Immunology', 'Laboratories',
                      'Medical Records', 'Billing', 'Neurology', 'Nutrition',
                      'Oncology', 'Occupational Therapy', 'Pharmacy',
                      'Physical Therapy', 'Plastic Surgery', 'Psychiatry',
                      'Sports Medicine', 'Social Work', 'Urology']


JOB_FUNCTION_CHOICES = ['Anesthesiologist', 'Anesthesiology Fellow',
                        'Nurse Anesthetist', 'Registered Respiratory Therapist',
                        'Cardiologist', 'Cardiology Fellow', 'Intensivist',
                        'Neonatologist', 'Critical Care Nurse Practitioner',
                        'Critical Care Registered Nurse',
                        'Critical Care Respiratory Therapist',
                        'Emergency physician', 'Emergency Nurse Practitioner',
                        'Emergency Physician Assistant', 'Flight Nurse',
                        'Nurse', 'Respiratory Therapist', 'EMT - Critical Care',
                        'EMT - Paramedic', 'EMT - Intermediate', 'EMT - Basic',
                        'Endocrinologist', 'Geriatrician',
                        'Gerontological Nurse Practitioner',
                        'Gastroenterologist', 'Haematologist',
                        'Medical Laboratory Technician', 'Phlebotomist',
                        'Nephrologist', 'Neurologist', 'Oncologist',
                        'Ophthamologist', 'Otolaryngologist',
                        'Ear, Nose and Throat physician', 'Pastoral Care',
                        'Healthcare Chaplain', 'Pulmonologist',
                        'Pulmonology Fellow',
                        'Registered Respiratory Therapist',
                        'Respiratory Therapist', 'Family Practice Physician',
                        'Internist', 'Family Nurse Practitioner',
                        'Physician Assistant', 'Pharmacist', 'Neonatalogist',
                        'Pediatrician', 'Neonatal Nurse Practitioner',
                        'Pediatric Physician Assistant',
                        'Pediatric Nurse Practitioner', 'Pediatric Nurse',
                        'Pediatric Respiratory Therapist', 'Psychiatrist',
                        'Psychologist', 'Psychiatric Nurse Practitioner',
                        'Mental Health Nurse Practitioner',
                        'Orthopedic Physician', 'Physical Therapist',
                        'Occupational Therapist', 'Physical Therapy Assistant',
                        'Occupational Therapy Assistant',
                        'Orthopaedic Technologist', 'Prosthetist',
                        'Orthotist', 'Radiotherapist', 'Radiation Therapist',
                        'Therapeutic Radiographer', 'Radiologist',
                        'Radiographer', 'Radiologic Technologist',
                        'CT Radiographer', 'Interventional Radiographer',
                        'Mammographer', 'Neuroradiographer',
                        'Medical Dosimetry Technologist',
                        'Radiation Protection Supervisor',
                        'Radiologist Practitioner Assistant',
                        'Reporting Radiographer', 'Sonographer', 'Obstetrician',
                        'Women\'s Health Nurse Practitioner',
                        'Nurse-Midwife', 'Doula', 'General Surgeon',
                        'Bariatric Surgeon', 'Cardiothoracic surgeon',
                        'Cardiac Surgeon', 'Hepatic Biliary Pancreatic Surgeon',
                        'Neurosurgeon']

COURSE_CHOICES = ['Getting Started', 'Intro to Radiation',
                  'Safety in the Workplace', 'Fire Procedure',
                  'Intro to Sanitation', 'Compliance Training',
                  'Practice Management', 'Educational Partnerships',
                  'Phlebotomy', 'IV Therapy', 'Nurse Aide',
                  'Computer Record Keeping', 'Anesthesia Simulation',
                  'Labor and Delivery Simulation',
                  'Training for Clinical Specialties',
                  'Intro to Medical Records', 'Intro to Billing']

from faker.providers.lorem import Provider as LoremProvider

class HSTMProvider(LoremProvider):
    """ Extra providers needed for HSTM """
    def department_choices(self):
        return self.word(ext_word_list=DEPARTMENT_CHOICES)

    def course_choices(self):
        return self.word(ext_word_list=COURSE_CHOICES)

    def job_function_choices(self):
        return self.word(ext_word_list=JOB_FUNCTION_CHOICES)


from faker.providers.internet import Provider as InternetProvider

class EmailProvider(InternetProvider):
    def custom_ascii_safe_email(self):
        return self.ascii_safe_email()



class TestFakerAnonymizer(object):
    def test_anonymizer_with_NO_params(self):
        a = FakerAnonymizer("{fake:random_uppercase_letter}")

        assert a("Value") == a("Value")
        assert a("boo") in uppercase

        b = FakerAnonymizer("{fake:military_apo}")
        assert b("Value") == b("Value")
        assert b("boo") == b("boo")
        assert b("Value") != b("boo")

    def test_anonymizer_with_params(self):
        a = FakerAnonymizer("{fake:numerify|text=###}")
        assert a("Value") == a("Value")

        b = FakerAnonymizer(
            "{fake:lexify|text=???,letters=abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ}"
        )
        assert len(b("value"))

        # Show we handle booleans
        before_today = FakerAnonymizer(
            "{fake:date_this_century|before_today=True,after_today=False}"
        )
        after_today = FakerAnonymizer(
            "{fake:date_this_century|before_today=False,after_today=True}"
        )

        # FakerAnonymizer always returns a string
        today = str(date.today())
        for let in "abcdefghijklmnopq":
            assert before_today(let) < today
            assert after_today(let) > today

    def test_anonymizer_with_postprocessor(self):
        # FakerAnonymizer always returns string unless converted
        a = FakerAnonymizer("{fake:ean8}")

        assert isinstance(a("Value"), basestring)

        b = FakerAnonymizer("{fake:ean8}", postprocessor=lambda x: int(x))

        assert isinstance(b("Value"), int)

        assert int(a("Value")) == b("Value")

    def test_anonymizer_with_provider(self):
        """Register a provider"""
        a = FakerAnonymizer("{fake:moo}", providers=[CowProvider])

        assert isinstance(a("Value"), basestring)
        assert a("Value") == "moo"

    def test_anonymizer_with_bad_providers(self):
        """Register a provider"""
        a = FakerAnonymizer("{fake:moo}", providers=[None, 4, CowProvider])

        assert isinstance(a("Value"), basestring)
        assert a("Value") == "moo"

    def test_anonymizer_with_stringprovider(self):
        """Register a string provider that is dynamically imported"""
        a = FakerAnonymizer("{fake:foo}", providers=["recipe.utils.TestProvider"])

        assert isinstance(a("Value"), basestring)
        assert a("Value") == "foo"

    def test_anonymizer_with_multipleproviders(self):
        """Register multiple providers"""
        a = FakerAnonymizer(
            "{fake:foo} {fake:moo}",
            providers=["recipe.utils.TestProvider", CowProvider],
        )

        assert isinstance(a("Value"), basestring)
        assert a("Value") == "foo moo"
