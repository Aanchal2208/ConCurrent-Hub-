import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 201, 409));

const baseUrl = (__ENV.BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '');
const password = __ENV.K6_PASSWORD || 'ConcurrentHubK6Test!';
const expectedConflicts = new Counter('expected_version_conflicts');
const unexpectedHotUpdateStatuses = new Counter('unexpected_hot_update_statuses');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  thresholds: {
    http_req_failed: ['rate<0.05'],
    checks: ['rate>0.99'],
  },
};

function requireResponse(response, expectedStatus, description) {
  if (response.status !== expectedStatus) {
    fail(`${description} returned HTTP ${response.status}`);
  }
}

export function setup() {
  const username = `k6_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
  const email = `${username}@example.test`;
  const registration = http.post(
    `${baseUrl}/auth/register`,
    JSON.stringify({
      username,
      email,
      password,
      full_name: 'ConcurrentHub k6 load test',
      role: 'editor',
    }),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'POST /auth/register' },
    },
  );
  requireResponse(registration, 201, 'Registration');

  const login = http.post(
    `${baseUrl}/auth/login`,
    JSON.stringify({ username, password }),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'POST /auth/login' },
    },
  );
  requireResponse(login, 200, 'Login');
  const token = login.json('access_token');
  if (!token) {
    fail('Login response did not contain an access token');
  }

  const headers = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
  const hotTaskResponse = http.post(
    `${baseUrl}/tasks`,
    JSON.stringify({
      title: 'k6 shared-task conflict target',
      assignee: username,
    }),
    { headers, tags: { name: 'POST /tasks (shared setup)' } },
  );
  requireResponse(hotTaskResponse, 201, 'Shared conflict-target task creation');
  const hotTask = hotTaskResponse.json();

  return {
    token,
    username,
    hotTaskId: hotTask.id,
    hotTaskVersion: hotTask.version,
  };
}

let ownTaskId;

export default function (data) {
  const headers = {
    Authorization: `Bearer ${data.token}`,
    'Content-Type': 'application/json',
  };

  const listResponse = http.get(
    `${baseUrl}/tasks?assignee=${encodeURIComponent(data.username)}`,
    { headers, tags: { name: 'GET /tasks' } },
  );
  check(listResponse, { 'list tasks returns 200': (response) => response.status === 200 });

  if (ownTaskId === undefined) {
    const createResponse = http.post(
      `${baseUrl}/tasks`,
      JSON.stringify({
        title: `k6 benchmark task vu${__VU}`,
        description: 'Created once per virtual user for this benchmark run',
        assignee: data.username,
      }),
      { headers, tags: { name: 'POST /tasks' } },
    );
    const created = check(createResponse, {
      'create task returns 201': (response) => response.status === 201,
    });
    if (!created) {
      return;
    }
    ownTaskId = createResponse.json('id');
  }

  const getResponse = http.get(`${baseUrl}/tasks/${ownTaskId}`, {
    headers,
    tags: { name: 'GET /tasks/{id}' },
  });
  check(getResponse, {
    'get task returns 200': (response) => response.status === 200,
  });
  if (getResponse.status !== 200) {
    return;
  }
  const task = getResponse.json();

  const updateResponse = http.put(
    `${baseUrl}/tasks/${task.id}`,
    JSON.stringify({
      expected_version: task.version,
      title: `k6 updated vu${__VU}-iteration${__ITER}`,
    }),
    { headers, tags: { name: 'PUT /tasks/{id} (independent)' } },
  );
  check(updateResponse, {
    'update independent task returns 200': (response) => response.status === 200,
  });

  const hotUpdateResponse = http.put(
    `${baseUrl}/tasks/${data.hotTaskId}`,
    JSON.stringify({
      expected_version: data.hotTaskVersion,
      title: `k6 shared update vu${__VU}-iteration${__ITER}`,
    }),
    { headers, tags: { name: 'PUT /tasks/{id} (shared)' } },
  );
  if (hotUpdateResponse.status === 409) {
    expectedConflicts.add(1);
    check(hotUpdateResponse, {
      'shared-task update returns expected 409 conflict': (response) =>
        response.json('status') === 'VERSION_CONFLICT',
    });
  } else if (hotUpdateResponse.status !== 200) {
    unexpectedHotUpdateStatuses.add(1);
    check(hotUpdateResponse, {
      'shared-task update returns 200 or expected 409': (response) =>
        response.status === 200,
    });
  }
}
