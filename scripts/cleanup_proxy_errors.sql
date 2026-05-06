BEGIN;

DELETE FROM host_http_observation
WHERE status = 'proxy-error';

DELETE FROM host_tls_observation
WHERE status = 'proxy-error';

DELETE FROM host_ssh_observation
WHERE status = 'proxy-error';

DELETE FROM host_service_observation
WHERE status = 'proxy-error';

COMMIT;
