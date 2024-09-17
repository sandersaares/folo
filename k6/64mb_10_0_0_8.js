import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
};

export default function() {
  http.get('http://10.0.0.8:1234/64mb');
}
