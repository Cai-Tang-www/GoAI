import { describe, expect, it } from 'vitest'
import { buildInterruptResume } from './agui'

describe('buildInterruptResume', () => {
  it('marks an approved resolution in the payload', () => {
    expect(buildInterruptResume('approval', '{"comment":"ok"}', 'approved')).toEqual({
      interruptId: 'approval',
      status: 'resolved',
      payload: { comment: 'ok', approved: true },
    })
  })

  it('marks a rejected resolution in the payload', () => {
    expect(buildInterruptResume('approval', '{}', 'rejected')).toEqual({
      interruptId: 'approval',
      status: 'resolved',
      payload: { approved: false },
    })
  })

  it('cancels without parsing the optional payload', () => {
    expect(buildInterruptResume('approval', 'not-json', 'cancelled')).toEqual({
      interruptId: 'approval',
      status: 'cancelled',
    })
  })

  it('rejects non-object resolution payloads', () => {
    expect(() => buildInterruptResume('approval', '[]', 'approved')).toThrow('Payload must be a JSON object')
  })
})
