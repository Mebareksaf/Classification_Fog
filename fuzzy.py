import numpy as np
from numpy.lib.function_base import average
import math



def abndObj(box,time):
        
        return {'name':box['class'], 'G':[box['x']/2,box['y']/2], 'x':box['x'], 'y':box['y'], 'time':time}

def sameplace(Nx,Ny,Ox,Oy):
    x1 = Nx/2
    x2 = Ox/2
    y1 = Ny/2
    y2 = Oy/2
    d = math.sqrt((x1-x2)**2+(y1-y2)**2)
    if d < 1 :
        return True

def DiffTime(Nt,Ot):
    return Nt - Ot

def comparObj(obj1, obj2):
    if sameplace(obj1['x'],obj1['y'],obj2['x'],obj2['y']): return True

def comparTimeObj(obj1,obj2):
    return float(obj2['time']-obj1['time'])

def fuzzy(firstPara, secondPara,X1,X2):
    
    #Do fuzzy system here

    return value
