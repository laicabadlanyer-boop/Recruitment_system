/**
 * Comprehensive client-side form validation
 */

(function() {
    'use strict';

    // Validation rules
    const validators = {
        required: function(value, field) {
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                return `${field} is required.`;
            }
            return null;
        },
        
        email: function(value, field) {
            if (value && value.trim()) {
                const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
                if (!emailRegex.test(value)) {
                    return `Please enter a valid ${field.toLowerCase()}.`;
                }
            }
            return null;
        },
        
        phone: function(value, field) {
            if (value && value.trim()) {
                const cleaned = value.replace(/[\s\-\(\)\+]/g, '');
                if (!/^\d+$/.test(cleaned) || cleaned.length < 10) {
                    return `Please enter a valid ${field.toLowerCase()}.`;
                }
            }
            return null;
        },
        
        minLength: function(value, field, min) {
            if (value && value.length < min) {
                return `${field} must be at least ${min} characters long.`;
            }
            return null;
        },
        
        maxLength: function(value, field, max) {
            if (value && value.length > max) {
                return `${field} must not exceed ${max} characters.`;
            }
            return null;
        },
        
        password: function(value, field) {
            if (value) {
                if (value.length < 6) {
                    return `${field} must be at least 6 characters long.`;
                }
                if (value.length > 128) {
                    return `${field} is too long (maximum 128 characters).`;
                }
            }
            return null;
        },
        
        match: function(value, field, matchValue, matchField) {
            if (value !== matchValue) {
                return `${field} does not match ${matchField || 'the confirmation'}.`;
            }
            return null;
        },
        
        number: function(value, field) {
            if (value && isNaN(value)) {
                return `${field} must be a valid number.`;
            }
            return null;
        },
        
        positive: function(value, field) {
            if (value && parseFloat(value) <= 0) {
                return `${field} must be a positive number.`;
            }
            return null;
        }
    };

    // Show field error
    function showFieldError(input, message) {
        const fieldContainer = input.closest('.form-group, div') || input.parentElement;
        let errorElement = fieldContainer.querySelector('.field-error');
        
        if (!errorElement) {
            errorElement = document.createElement('div');
            errorElement.className = 'field-error text-red-400 text-xs mt-1 flex items-center gap-1';
            errorElement.innerHTML = `<i class="fas fa-exclamation-circle"></i><span>${message}</span>`;
            fieldContainer.appendChild(errorElement);
        } else {
            errorElement.innerHTML = `<i class="fas fa-exclamation-circle"></i><span>${message}</span>`;
        }
        
        input.classList.add('border-red-500', 'focus:border-red-500', 'focus:ring-red-500');
        input.classList.remove('border-gray-700', 'focus:border-red-500');
    }

    // Clear field error
    function clearFieldError(input) {
        const fieldContainer = input.closest('.form-group, div') || input.parentElement;
        const errorElement = fieldContainer.querySelector('.field-error');
        
        if (errorElement) {
            errorElement.remove();
        }
        
        input.classList.remove('border-red-500', 'focus:border-red-500', 'focus:ring-red-500');
        input.classList.add('border-gray-700');
    }

    // Validate single field
    function validateField(input) {
        const rules = input.dataset.validate ? input.dataset.validate.split('|') : [];
        const fieldName = input.getAttribute('name') || input.id || 'Field';
        const displayName = input.dataset.label || fieldName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        const value = input.value;
        
        clearFieldError(input);
        
        for (const rule of rules) {
            const [ruleName, ...params] = rule.split(':');
            const validator = validators[ruleName];
            
            if (validator) {
                const error = validator(value, displayName, ...params);
                if (error) {
                    showFieldError(input, error);
                    return false;
                }
            }
        }
        
        // Special handling for password confirmation
        if (input.type === 'password' && input.name.includes('confirm')) {
            const passwordField = input.form.querySelector('input[name="password"], input[name="new_password"], input[name="current_password"]');
            if (passwordField && passwordField.value !== value) {
                showFieldError(input, 'Passwords do not match.');
                return false;
            }
        }
        
        return true;
    }

    // Validate entire form
    function validateForm(form) {
        let isValid = true;
        const inputs = form.querySelectorAll('input[data-validate], select[data-validate], textarea[data-validate]');
        
        inputs.forEach(input => {
            if (!validateField(input)) {
                isValid = false;
            }
        });
        
        return isValid;
    }

    // Initialize form validation
    function initFormValidation() {
        document.querySelectorAll('form').forEach(form => {
            // Validate on submit
            form.addEventListener('submit', function(e) {
                if (!validateForm(form)) {
                    e.preventDefault();
                    e.stopPropagation();
                    
                    // Focus first invalid field
                    const firstError = form.querySelector('.field-error');
                    if (firstError) {
                        const input = firstError.parentElement.querySelector('input, select, textarea');
                        if (input) {
                            input.focus();
                            input.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        }
                    }
                    
                    return false;
                }
            });
            
            // Validate on blur
            form.querySelectorAll('input[data-validate], select[data-validate], textarea[data-validate]').forEach(input => {
                input.addEventListener('blur', function() {
                    validateField(input);
                });
                
                // Clear error on input
                input.addEventListener('input', function() {
                    if (input.classList.contains('border-red-500')) {
                        validateField(input);
                    }
                });
            });
        });
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initFormValidation);
    } else {
        initFormValidation();
    }

    // Export for global use
    window.formValidation = {
        validateField: validateField,
        validateForm: validateForm,
        showError: showFieldError,
        clearError: clearFieldError
    };
})();

