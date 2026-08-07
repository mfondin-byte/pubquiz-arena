#!/usr/bin/env python3
"""Smoke test for PubQuiz Arena backend."""

import subprocess, time, sys, http.client, json

proc = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'main:app', '--host', '0.0.0.0', '--port', '8099'], 
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(4)

def req(method, path, body=None):
    conn = http.client.HTTPConnection('127.0.0.1', 8099)
    headers = {'Content-Type': 'application/json'} if body else {}
    conn.request(method, path, json.dumps(body).encode() if body else None, headers=headers)
    resp = conn.getresponse()
    data = resp.read().decode()
    conn.close()
    return resp.status, json.loads(data) if data else ''

try:
    # 1. Create a quiz
    status, data = req('POST', '/api/quizzes', {'name': 'Test Quiz', 'max_teams': 3, 'points_per_tick': 5})
    assert status == 200, f"Expected 200, got {status}"
    assert data['quiz']['name'] == 'Test Quiz'
    print('1. Create quiz: PASS')

    # 2. Add a question
    status, qdata = req('POST', '/api/quizzes/Test%20Quiz/questions', {
        'question_type': 'text',
        'question_text': 'What is the capital of France?',
        'category': 'Geography',
        'timer_seconds': 15,
        'options': ['Paris', 'London', 'Berlin', 'Madrid'],
        'answer': 0
    })
    assert status == 200, f"Expected 200, got {status}"
    assert qdata['question']['question_text'] == 'What is the capital of France?'
    assert qdata['question']['answer'] == 0
    print('2. Add question: PASS')

    # 3. Get quiz
    status, data = req('GET', '/api/quizzes/Test%20Quiz')
    assert status == 200, f"Expected 200, got {status}"
    assert len(data['questions']) == 1
    print('3. Get quiz: PASS')

    # 4. List quizzes
    status, data = req('GET', '/api/quizzes')
    assert status == 200, f"Expected 200, got {status}"
    assert len(data['quizzes']) >= 1
    print('4. List quizzes: PASS')

    # 5. Get questions for quiz
    status, data = req('GET', '/api/quizzes/Test%20Quiz/questions')
    assert status == 200, f"Expected 200, got {status}"
    assert isinstance(data, list)
    assert len(data) == 1
    print('5. Get questions: PASS')

    # 6. Delete quiz
    status, data = req('DELETE', '/api/quizzes/Test%20Quiz')
    assert status == 200, f"Expected 200, got {status}"
    print('6. Delete quiz: PASS')

    # 7. Ensure demo quiz exists
    status, data = req('GET', '/api/quizzes/General%20Knowledge')
    assert status == 200, f"Expected 200, got {status}"
    assert len(data['questions']) == 10
    print('7. Demo quiz exists with 10 questions: PASS')

    print('\nAll API tests PASSED')

except AssertionError as e:
    print(f'FAIL: {e}')
except Exception as e:
    print(f'Error: {e}')
finally:
    proc.terminate()
    proc.wait()
