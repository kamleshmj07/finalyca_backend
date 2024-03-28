import os
from datetime import datetime, timedelta
from utils.utils import get_config

from utils.finalyca_store import *
from fin_models.servicemanager_models import DeliveryRequest, DeliveryManager

def delete_old_delivery_requests(db_session):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)

    cur_date_time = datetime.now()
    exp_date = cur_date_time - timedelta(minutes=config['REPORT_EXPIRY_PERIOD_IN_MIN'])

    update_values = {
        DeliveryRequest.Is_Deleted : 1
    }

    sql_users_delivery_requests = db_session.query(DeliveryRequest)\
                                    .filter(DeliveryRequest.Is_Deleted != 1,
                                            DeliveryRequest.Type == 'X-Ray PDF',
                                            DeliveryRequest.Completion_Time <= datetime.strftime(exp_date, '%Y-%m-%d %H:%M:%S'))\
                                    .update(update_values)
    db_session.commit()
    



if __name__ == '__main__':
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    delete_old_delivery_requests(db_session)