import http from 'k6/http';

export const options = {
  noConnectionReuse: true,
};

export default function () {
  http.get('http://10.0.0.8:1234/64mb');
}
