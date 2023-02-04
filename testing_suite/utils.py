import os

def clean_dir() -> None:
    path = os.getcwd()
    for file in os.listdir(path):
        if file.endswith('.log') or file.endswith('.csv'):
            os.remove(os.path.join(path, file))
    
if __name__ == '__main__':
    clean_dir()
    print('Cleaned directory')