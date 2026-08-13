import { describe, expect, it } from 'vitest'
import { ApiError } from '../api/client'
import { isFormValidationError } from './notify'

describe('isFormValidationError', () => {
  it.each(['VALIDATION_FAILED', 'INVALID_ID'])('treats %s as an inline form error', (code) => {
    expect(isFormValidationError(new ApiError('invalid input', 400, code, 'trace_1'))).toBe(true)
  })

  it('leaves state and permission failures for the global request notification', () => {
    expect(isFormValidationError(new ApiError('forbidden', 403, 'AUTH_FORBIDDEN', 'trace_2'))).toBe(false)
    expect(isFormValidationError(new Error('bad json'))).toBe(false)
  })
})
