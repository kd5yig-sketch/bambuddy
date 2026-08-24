/**
 * Tests that PrintModal hides Bambu/AMS-only UI (filament mapping, print
 * options / calibration toggles) when every selected printer is Klipper —
 * see isKlipperTarget in components/PrintModal/index.tsx.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import { render } from '../utils';
import { PrintModal } from '../../components/PrintModal';
import { http, HttpResponse } from 'msw';
import { server } from '../mocks/server';

const klipperPrinter = {
  id: 1,
  name: 'Voron 2.4',
  protocol: 'klipper',
  ip_address: '192.168.1.200',
  enabled: true,
  is_active: true,
};

const bambuPrinter = {
  id: 2,
  name: 'X1 Carbon',
  model: 'X1C',
  ip_address: '192.168.1.100',
  enabled: true,
  is_active: true,
};

describe('PrintModal Klipper target', () => {
  const mockOnClose = vi.fn();
  const mockOnSuccess = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    server.use(
      http.get('/api/v1/printers/', () => {
        return HttpResponse.json([klipperPrinter, bambuPrinter]);
      }),
      http.get('/api/v1/archives/:id/plates', () => {
        return HttpResponse.json({ is_multi_plate: false, plates: [] });
      }),
      http.get('/api/v1/archives/:id/filament-requirements', () => {
        return HttpResponse.json({ filaments: [] });
      }),
      http.get('/api/v1/printers/:id/status', () => {
        return HttpResponse.json({ connected: true, state: 'IDLE', ams: [], vt_tray: [] });
      }),
      http.post('/api/v1/queue/', () => {
        return HttpResponse.json({ id: 1, status: 'pending' });
      })
    );
  });

  it('hides Print Options for an all-Klipper selection', async () => {
    render(
      <PrintModal
        mode="create"
        archiveId={1}
        archiveName="Benchy"
        initialSelectedPrinterIds={[1]}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
      />
    );

    // Give the printer-selection-dependent panels a chance to settle before
    // asserting an absence (a false negative from asserting too early would
    // be worse than a slow test).
    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Print' })).toBeInTheDocument();
    });

    expect(screen.queryByText('Print Options')).not.toBeInTheDocument();
  });

  it('still shows Print Options for a Bambu-only selection', async () => {
    render(
      <PrintModal
        mode="create"
        archiveId={1}
        archiveName="Benchy"
        initialSelectedPrinterIds={[2]}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
      />
    );

    await waitFor(() => {
      expect(screen.getByText('Print Options')).toBeInTheDocument();
    });
  });
});
