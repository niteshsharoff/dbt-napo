{% docs int_training_payment_events %}

A best effort reconstruction of training domain events using a combination of raw data sources from backend and Stripe.

Event type can be one of the following:
* trial_started - When a customer's free trial starts.
* payment_intent - When a Stripe payment intent is created for a customer.
* cancelled - When a Stripe subscription is cancelled.
* trial_ended - When a customer's free trial expires.
* pa_registration - When a customer with source = 'pa_registrant' is added to booking-service DB.

Event timestamp is defined as follows:
* trial_started - The 'trial_started_at' field on a Stripe subscription.
* payment_intent - The timestamp at which a Stripe payment intent is created.
* cancelled - The 'cancelled_at' field on a Stripe subscription.
* trial_ended - The 'trial_ended_at' field on a Stripe subscription.
* pa_registration - The 'created_at' column of a customer row in booking-service DB.

{% enddocs %}