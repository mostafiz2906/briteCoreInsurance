from django import forms
from .models import input_file

class DocumentForm(forms.ModelForm):
    class Meta:
        model = input_file
        fields = ('file_name', 'document', )
