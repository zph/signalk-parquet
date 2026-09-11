// Archive ownership and replica advice uses textContent only; remote strings are never HTML.
(() => {
  async function update() {
    const panel = document.getElementById('archive-mode-notice');
    if (!panel) return;
    try {
      const response = await fetch(
        '/plugins/signalk-parquet/api/archive-status',
        { credentials: 'same-origin', cache: 'no-store' }
      );
      if (!response.ok) return;
      const status = await response.json();
      const text =
        status.mode === 'replica'
          ? 'S3-only replica: live Signal K capture, local imports, aggregation and uploads are disabled. History comes from verified archive snapshots. Offline or missing periods raise standard Signal K alerts and catch up when connectivity returns.'
          : status.mode === 'producer'
            ? 'Authoritative producer: this instance owns the shared S3 archive. Configure other instances using the same endpoint, bucket and prefix as replicas. A conflicting producer cannot take ownership automatically.'
            : 'Shared S3 archive: choose exactly one authoritative producer and use S3-only replica mode on other servers. Endpoint, bucket and prefix must all match. Configure Archive role in Signal K plugin settings.';
      panel.textContent =
        text +
        (status.state === 'blocked'
          ? ' Producer ownership is blocked; select replica mode for an archive owned by another server.'
          : '');
      panel.hidden = false;
    } catch {
      /* Keep the previous advice during a transient network failure. */
    }
  }
  update();
  setInterval(update, 60000);
})();
