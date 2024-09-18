import http from 'k6/http';

export const options = {
  noConnectionReuse: true,
};

export default function () {
  http.get('http://localhost:1234/20kb');
}
