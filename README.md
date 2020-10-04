# Google Cloud Python 
Python programs delivered by big data engineer team of the ngbi project that help other teams to have the information available to carry out their project tasks.

## Before Start
1) Validate that you are using python 3
2) Create a virtual environment
3) Install the packages described in the application for the desired python program.

## Creating a virtual environment
First we have to install ' python3-env' with the following coomand:
````
sudo apt install python3-venv
````
Create virtual environment
````
python3 -m venv venv
````
 Remember to add venv/ folder at .gitignore list
 
To activate the virtual environment:
```
source venv/bin/activate
```

In order to add new packages o our new virtual environment (venv) we create a file called 'requirements.txt' and execute the following command:
````
pip install -r requirements.txt
````

## Export data from bq
**Script name:** cp_ptc.py <br/> 
**dependency:** move_file-cp.sh