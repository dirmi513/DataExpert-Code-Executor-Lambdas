import traceback 
import sys 
import io 
from io import StringIO 
import base64
import json 
import logging
import re 
import boto3
import pandas as pd
import numpy as np
import matplotlib 
import re 
import os 

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(handler)


def execute_code(code):
    """Executes a string containing Python code.

    Args:
        code: The code to execute. 

    Returns:
        If the client's code has no errors, the function
        will return the dictionary of local variables declared
        in the client's code, as well as the output of any print
        statements used.
        If the code has errors, will return a string containing
        the type of Exception that occurred, along with its line
        number and details.
    """
    variables =  {}
    error = False 
    old_stderr = sys.stderr
    old_stdout = sys.stdout
    redirected_output = sys.stdout = StringIO()
    try:
        exec(code, variables) 
    except SyntaxError as e:
        redirected_error = sys.stderr = StringIO()
        cl, exc, tb = sys.exc_info()
        traceback.print_exception(cl, exc, tb)
        traceback_output = redirected_error.getvalue()
        traceback_output = traceback_output[re.search(r'File "<string>"', traceback_output).start():]
        traceback_output = 'Traceback (most recent call last):\n  ' + traceback_output.replace('<string>', 'script.py')
        error = True
    except Exception as e:  
        code_list = code.split('\n') 
        traceback_output = 'Traceback (most recent call last):\n'
        error_class = e.__class__.__name__
        detail = e.args[0] 
        cl, exc, tb = sys.exc_info()   
        tb = tb.tb_next 
        tracebacks = traceback.extract_tb(tb)  
        for frame in tracebacks:
            traceback_str = f'  File "script.py", line {frame.lineno}, in {frame.name}\n'
            code_line = '    ' + code_list[frame.lineno - 1].strip() + '\n'
            traceback_output += traceback_str + code_line
        traceback_output += '\n' + error_class + ': ' + detail
        error = True 
    sys.stdout = old_stdout 
    sys.stderr = old_stderr  
    if error:
        logger.info(traceback_output)
        traceback_output = traceback_output.replace('<', '&lt;').replace('>', '&gt;')
        traceback_output = '<pre class="traceback-output">' + traceback_output + '</pre>' 
        return traceback_output, False 
    return variables, redirected_output


def print_statement_output(output, output_string, output_list):
    """Gather all of the print statement output from the StringIO object.

    Args:
        output: The StringIO object containing print statement output.
        output_string: The output string that contains all of the HTML
            to be sent back to the client.
        output_list: The list of output strings, which will be used to
            check the client's code against the correct answer.
    """  
    print_output = output.getvalue().split('\n')[:-1]
    if len(print_output) == 1:
        output_string += ('<p class="output-text-p">' + print_output[0] + '</p>')
        output_list.append(print_output[0])
    else:
        for i, element in enumerate(print_output):
            if i == 0:
                output_string += ('<p class="output-text-p">' + element + '<br>')
            elif i == len(print_output)-1:
                output_string += (element + '</p>')
            else: 
                output_string += (element + '<br>') 
            output_list.append(element)
    return output_string, output_list  


def s3_download_file(filename, bucket):
    """Downloads an S3 file from the specified bucket to the /tmp/ directory of the lambda container.

    Args:
        filename: Name of the file, with its extension.
        bucket: S3 bucket that contains the file. 
    """
    s3 = boto3.client('s3')
    if filename in [key['Key'] for key in s3.list_objects(Bucket=bucket)['Contents']]:
        s3.download_file(bucket, filename, '/tmp/' + filename)
    else: 
        return f'<p class="output-text-p">FileError: Incorrect file name \'{filename}\'</p>'


def s3_downloads(code):
    """Downloads any CSV files that are defined in the client's code.

    Args: 
        code: A string containing Python code. 

    Returns:
        If the file(s) specified don't exist, return that as an error string.
        If the code is missing a '/tmp/' before the file name, return that
        as an error string.
        If there is no file downloading or the files are on S3, download them
        and return None.
    """
    # If there is a pd.read_csv() or open() expression in the client's code, try downloading the file
    # If the file doesn't exist in S3, don't execute client's code and return that as an error message
    download_files = re.findall(r"(?:pd.read_csv|open)\('/tmp/([\w\.-]+)'", code)
    if download_files:
        for file_name in download_files: 
            data_download = s3_download_file(file_name, 'dataexpert.datasets')
            if data_download is not None:
                return data_download  
    # If the client forgets to put the /tmp/ in front of the csv file in the pd.read_csv() or open() function 
    tmp_missing = re.findall(r"(pd.read_csv|open)\('(?<!/tmp/)([\w\.-]+)'", code)
    if tmp_missing: 
        first_instance = tmp_missing[0]  # Only show the error message for the first instance
        func = first_instance[0]
        csv = first_instance[1]
        error_message = f"""
        <p class="output-text-p">
            Missing the /tmp/ before the csv file name in {func}('{csv}'). 
            <br><br>
            It should be {func}('/tmp/{csv}') 
            <br><br> 
            Please try again!
        <p>
        """
        return error_message 


def s3_download_correct_answer(_cls):
    """Downloads the coding challenge answer for a slide from the specified S3 bucket 
        to the /tmp/ directory of the lambda container.

    Args:
        _cls: The course + lesson + slide integer combo for the slide. 
    """
    filename = str(_cls) + '.json'
    s3_download_file(filename, 'dataexpert.correct.answers')
    with open('/tmp/' + filename) as f: 
        correct_answer_dic = json.load(f)
    return correct_answer_dic


def graph_html(plot):
    """Converts a Matplotlib plot object into an HTML image.

    Args:
        plot: Matplotlib plot object.
    """
    img = io.BytesIO()
    plot.savefig(img, format='png', bbox_inches='tight')
    img.seek(0)
    b64 = base64.b64encode(img.getvalue()).decode('utf-8')
    html = f'<img src="data:image/png;base64, {b64}">' 
    return html


def parse_local_vars(local_vars, output_string, answer_dic): 
    """Loops through the local variables declared by client in their code and generates HTML output of them.

    Args:
        local_vars: Dictionary of local variables declared by client's code string.
        output_string: HTML string that will be sent back to the client.
        answer_dic: Dictionary that will be used to compare client's code execution
            results to the correct answer.

    Returns:
        An HTML string that will be sent back to the client, containing
        an arrowed list of variables and their values.
    """
    line_separator = '<hr id="hr-right-pane">'
    variable_title = '<p class="code-output-title">Variables</p>'
    graphs = '<p class="code-output-title">Plots</p> '
    len_graph_title = len(graphs)
    is_fig = False
    fig_var = ''
    answer_graphs = []
    exceptions = ['module', 'function', 'type']  
    len_local = 0   
    for variable, value in local_vars.items(): 
        html = """ 
        <div class="main"> 
            <svg 
                class="arrow" 
                viewBox="0 0 100 100" 
                width="10px"
            > 
                <polygon 
                    points="-6.04047,17.1511 81.8903,58.1985 -3.90024,104.196" 
                    transform="matrix(0.999729, 0.023281, -0.023281, 0.999729, 7.39321, -10.0425)"
                > 
                </polygon> 
            </svg> 
            {} 
            <span class="type"> 
                {} 
            </span> 
            <div class="result"> 
                {} 
            </div> 
        </div>
        """ 
        if value.__class__.__name__ in exceptions or variable == '__builtins__':
            continue 
        if len_local == 0:
            output_string += variable_title 
            len_local += 1    
        # Variable name and class - will be inserted into html string 
        var_name = str(variable)
        var_class = (str(value.__class__.__name__) + ' ' + str(value.__class__).replace('<', '&lt;').replace('>', '&gt;'))  
        # Pandas & Numpy 
        if isinstance(value, pd.DataFrame):
            shape = ' ' + str(value.shape[0]) + ' Rows x ' + str(value.shape[1]) + ' Columns'
            var_class += shape 
            var_value = str(value.to_html(max_rows=10, max_cols=10))  
            html = html.format(var_name, var_class, var_value)
            answer_dic[variable] = value.to_json()
        elif isinstance(value, pd.Series):
            rows = ' ' + str(value.shape[0]) + ' Rows'
            var_class += rows  
            var_value = str(value.reset_index().to_html(max_rows=10, max_cols=10)) 
            html = html.format(var_name, var_class, var_value)
            answer_dic[variable] = value.to_json()
        elif isinstance(value, np.ndarray): 
            var_value = 'array(' + str(value).replace('\n ', ',<br>' + ('&emsp;' * 7)) +')'  
            html = html.format(var_name, var_class, var_value)
            answer_dic[variable] = value.tolist()
        # Matplotlib Plots 
        elif value.__class__.__name__ == 'Figure' and len(value.axes) > 0:
            graph_to_html = graph_html(value)
            graphs += graph_to_html
            is_fig = True
            fig_var += variable
            var_value = str(value).replace('<', '&lt;').replace('>', '&gt;')
            html = html.format(var_name, var_class, var_value)
            answer_graphs.append(graph_to_html)
            answer_dic[variable] = str(value)
        elif value.__class__.__name__ == 'AxesSubplot':
            var_value = str(value).replace('<', '&lt;').replace('>', '&gt;')
            html = html.format(var_name, var_class, var_value)
            answer_dic[variable] = str(value)
            if (is_fig and value not in local_vars[fig_var].axes) or (not is_fig):
                graph_to_html = graph_html(value.figure)
                graphs += graph_to_html
                answer_graphs.append(graph_to_html)
        # All others 
        else:
            var_value = str(value).replace('<', '&lt;').replace('>', '&gt;')
            html = html.format(var_name, var_class, var_value)
            answer_dic[variable] = str(value) 
        output_string += html 
    if len(graphs) > len_graph_title: 
        if len_local == 1: 
            output_string += line_separator
        output_string += graphs
        answer_dic['Plots'] = answer_graphs
    output_string = output_string.replace('\n', '').replace("\'","")
    return output_string, answer_dic


def upload_correct_answer_to_s3(_cls, answer_dic):
    """Uploads the correct answer for the slide to dataexpert.correct.answers S3 bucket.

    Args:
        _cls: The courseLessonSlide combo integer.
        answer_dic: Dictionary containing all variables and output of
            the correct answer.
    """
    json_file = f'{_cls}.json'
    file_path = os.path.join('/tmp/', json_file)
    with open(file_path, 'w') as f:
        json.dump(answer_dic, f) 
     
    correct_answer_bucket = 'dataexpert.correct.answers'
    s3 = boto3.client('s3') 
    s3.upload_file(Filename=file_path, Bucket=correct_answer_bucket, Key=json_file)


def lambda_handler(event, context):
    """Executes client's code and returns the output in HTML format.

    Args:
        event: A JSON object with code, grade and cls keys.
        context: Lambda Context runtime methods and attributes.

    Returns:
        The results of the code execution as a dictionary (if 
        there are no errors), or string (if errors) in HTML format.
    """
    code, grade_answer, _cls = event['code'], event['grade'], event['cls']
    submit_correct_answer = event['submit_correct_answer']
    output_dic, answer_dic = {}, {} 
    output_string = '' 
    output_title = '<p class="code-output-title">Output</p>' 
    line_separator = '<hr id="hr-right-pane">'    
    downloads = s3_downloads(code) 
    logger.info('S3 downloads for any files specified in code complete.')
    # If there is an issue downloading files specified in client's code, return the exception 
    if downloads is not None: 
        return downloads  
    client_variables, output = execute_code(code) 
    # If code execution fails, return the exception
    if type(client_variables) != dict:  
        return '<p class="output-text-p">' + client_variables + '</p>'
    logger.info('Code executed successfully.')
    if output.getvalue():
        output_list = []
        output_string += output_title
        output_string, output_list = print_statement_output(output, output_string, output_list)
        output_string += line_separator
        answer_dic['Output'] = output_list  
        logger.info('Print statement output captured.')
    output_string, answer_dic =  parse_local_vars(client_variables, output_string, answer_dic)
    logger.info('Code variable declarations parsed.')
    output_dic['output_code'] = output_string
    if submit_correct_answer == 'T':
        try:
            upload_correct_answer_to_s3(_cls, answer_dic)
            return {'Status': 'Success'}
        except Exception as e:
            logger.info(f'There was an error uploading the correct answer to S3: {e}')
            return {'Status': 'Error'}
    if grade_answer == 'T':
        correct_answer = s3_download_correct_answer(_cls)
        if answer_dic == correct_answer:
            output_dic['correct_answer'] = 'T'
            output_dic['cls'] = _cls
        else:
            output_dic['correct_answer'] = 'F' 
    return output_dic 