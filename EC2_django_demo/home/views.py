from django.shortcuts import render
from django.http import HttpResponse

def index(request):
    return HttpResponse("Hello World! This is Amit")


# Create your views here.
