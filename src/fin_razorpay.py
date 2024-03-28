from datetime import date, timedelta, datetime
from flask import jsonify, request, Blueprint, current_app, jsonify, request
from fin_models.controller_transaction_models import RazorpayLog
from fin_models.controller_master_models import Organization
from utils.utils import shift_date
from .access_bl import save_organization_from_prelogin, email_validation_for_free_trial, NotUniqueValueException, LicenceLimitExceedException
from bizlogic.newsletter import create_newsletter
from werkzeug.exceptions import BadRequest, Conflict

payment_bp = Blueprint("payment_bp", __name__)

@payment_bp.route('/buy', methods=["POST"])
def something():
    payload = request.json if request.json else request.form

    name = payload["name"]
    email = payload["email"]
    mobile = payload["mobile"]
    organization = payload["organization"]
    payment_model = payload["payment_model"]
    lite_license_count = int(payload["lite_license_count"])
    pro_license_count = int(payload["pro_license_count"])
    gst_no = payload.get("gst_no")

    pro_price = 0.
    if pro_license_count > 0:
        if pro_license_count == 1:
            pro_price = 10000
        elif pro_license_count > 1 and pro_license_count < 5:
            pro_price = (10000 - (pro_license_count * 1000)) * pro_license_count
        elif 5 <= pro_license_count < 10:
            pro_price = (10000 - (pro_license_count - 1) * 1000) * pro_license_count
        elif pro_license_count >= 10:
            pro_price = (10000 - (pro_license_count - 5) * 1000) * pro_license_count

    lite_price = 0.
    if lite_license_count > 0:
        if lite_license_count == 1:
            lite_price = 10000
        elif lite_license_count > 1 and lite_license_count < 5:
            lite_price = (10000 - (lite_license_count * 1000)) * lite_license_count
        elif 5 <= lite_license_count < 10:
            lite_price = (10000 - (lite_license_count - 1) * 1000) * lite_license_count
        elif lite_license_count >= 10:
            lite_price = (10000 - (lite_license_count - 5) * 1000) * lite_license_count

        lite_price -= lite_price * 0.6

    base_price = pro_price + lite_price

    final_price = 0.
    active_date = date.today()
    if payment_model == "monthly":
        license_expiry_date = shift_date(active_date, 1, 0)
        final_price = base_price
    elif payment_model == "quarterly":
        license_expiry_date = shift_date(active_date, 3, 0)
        # final_price = (base_price * 11) / 4
        final_price = (base_price * 10 * 1.25) / 4
    elif payment_model == "yearly":
        license_expiry_date = shift_date(active_date, 0, 1)
        final_price = base_price * 10
    else:
        raise BadRequest("Unknown time period provided")

    # Id is created
    try:
        org_id = save_organization_from_prelogin(current_app.store.db, organization, name, email, mobile, lite_license_count, pro_license_count, license_expiry_date, gst_no, current_app.config, current_app.jinja_env)
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    # For more info, refer to the documentation https://razorpay.com/docs/payments/orders/
    # https://razorpay.com/docs/api/orders/#create-an-order
    data = {
        # multiply the INR into paisa as razorpay needs the value in smallest subunit
        "amount": final_price * 100, 
        "currency": "INR", 
        "notes" : {
            "name": name,
            "email": email,
            "mobile" : mobile,
            "organization" : organization,
            "organization_id": org_id
        }
    }

    order = current_app.razor.order.create(data=data)
    order_id = order["id"]

    db_razor_log = RazorpayLog()
    db_razor_log.org_id = org_id
    db_razor_log.razorpay_order_id = order_id
    current_app.store.db.add(db_razor_log)
    current_app.store.db.commit()

    details = F"Payment for {lite_license_count+pro_license_count} licences"
    
    resp = {"details": details, "key_id": current_app.config["RAZORPAY_KEY_ID"] , "order_id": order_id, "name": name, "email": email, "contact": mobile}

    return jsonify(resp)

@payment_bp.route('/buy/callback', methods=["POST"])
def something_1():
    payload = request.json

    razorpay_order_id = payload['razorpay_order_id']
    razorpay_payment_id = payload['razorpay_payment_id']
    razorpay_signature = payload['razorpay_signature']

    is_verified = current_app.razor.utility.verify_payment_signature(payload)
    if not is_verified:
        raise BadRequest(F"{razorpay_order_id} with {razorpay_payment_id} could not be verified")

    order = current_app.razor.order.fetch(razorpay_order_id)
    payment = current_app.razor.payment.fetch(razorpay_payment_id)
    if order["status"] == 'paid':
        sql_razor = current_app.store.db.query(RazorpayLog).filter(RazorpayLog.razorpay_order_id == razorpay_order_id).one_or_none()
        if sql_razor:
            sql_razor.razorpay_order = order
            sql_razor.razorpay_payment = payment
            sql_razor.razorpay_payment_id = razorpay_payment_id
            sql_razor.razorpay_signature = razorpay_signature
            current_app.store.db.commit()

            sql_org = current_app.store.db.query(Organization).filter(Organization.Organization_Id == sql_razor.org_id).one_or_none()
            sql_org.Is_Active = True
            sql_org.is_payment_pending = False
            current_app.store.db.commit()
            
        # print(order)
        # print(payment)

    return jsonify("All ok.")

@payment_bp.route('/free_trial', methods=['POST'])
def free_trial():
    form = request.json if request.json else request.form
    org_name = form["organization"]
    mobile = form["mobile"]
    user_name = form["name"]
    email = form["email"]

    # set the license expiry date to 2 days from now, for free trial users
    # set pro license count to 1 and lite license count to 0, as we are giving platinum access to free trial users
    license_expiry_date = datetime.strftime(date.today() + timedelta(days=2), '%Y-%m-%d')
    lite_license_count = 0
    pro_license_count = 1

    try:
        # check if email is valid, i.e it should be from legit organization as we are not allowing free trial users for gmail, yahoo, outlook etc.
        email_validation_for_free_trial(email)
        
        # here we are calling the same function for free trial, which we use when user is self subscribing, but make user, for free trial set the usertype_id flag of organization to free trial 
        org_id = save_organization_from_prelogin(current_app.store.db, org_name, user_name, email, mobile, lite_license_count=lite_license_count, pro_license_count=pro_license_count, license_expiry_date=license_expiry_date, gst_no=None, config=current_app.config, jinja_env= current_app.jinja_env, is_free_trial=1)
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    return jsonify({"msg": F"Free trial access has been given to you. You can access finalyca portal", "status_code": 200})

@payment_bp.route('/newsletter', methods=['POST'])
def api_newsletter():
    """
    This Api will store the data the newsletter database , so we can send the newsletter mails and other things to user accordingly, but before storing the data check if present or not and send proper message to user
    """

    form = request.json if request.json else request.form

    try:
        newsletter_id = create_newsletter(current_app.store.db, form=form)
    except Exception as exe:
        raise Conflict(str(exe))

    return jsonify({"msg": 'You will now receive newsletter updates from finalyca', "status_code": 200})





