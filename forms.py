# forms.py (create this file if you don't have one)
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, TextAreaField, SubmitField, DateTimeLocalField
from wtforms.validators import DataRequired, Optional, Length

class LogActivityForm(FlaskForm):
    # Populated dynamically in the route
    account_card_code = SelectField('Account', validators=[DataRequired("Please select an account.")], choices=[])

    activity_type = SelectField('Activity Type',
                                validators=[DataRequired()],
                                choices=[('', '-- Select Type --'), ('Call', 'Call'), ('Visit', 'Visit')],
                                default='')

    # Make datetime optional, default handled in route
    activity_datetime = DateTimeLocalField('Date & Time of Activity (Optional)',
                                           format='%Y-%m-%dT%H:%M',
                                           validators=[Optional()])

    outcome = SelectField('Outcome (Optional)',
                           choices=[
                               ('', '-- Select Outcome --'),
                               ('Successful Contact', 'Successful Contact'),
                               ('Left Voicemail', 'Left Voicemail'),
                               ('No Answer', 'No Answer'),
                               ('Follow-up Needed', 'Follow-up Needed'),
                               ('Meeting Scheduled', 'Meeting Scheduled'),
                               ('Order Placed', 'Order Placed'),
                               ('General Check-in', 'General Check-in'),
                               ('Issue Resolution', 'Issue Resolution'),
                               ('Other', 'Other')
                           ], default='')

    notes = TextAreaField('Notes (Optional)', validators=[Optional(), Length(max=1000)])
    submit = SubmitField('Log Activity')