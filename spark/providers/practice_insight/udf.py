

def generate_place_of_service_std_id(
        claim_type,
        pos_cd,
        fclty_type_pos_cd,
        diag_cd_1,
        diag_cd_2,
        diag_cd_3,
        diag_cd_4,
        diag_code
):
    if claim_type is not None and claim_type == 'P':
        if pos_cd is not None and pos_cd != '' \
           and diag_code in [diag_cd_1, diag_cd_2, diag_cd_3, diag_cd_4]:
            return pos_cd
        elif fclty_type_pos_cd is not None and fclty_type_pos_cd != '':
            return fclty_type_pos_cd
    return None


def generate_inst_type_of_bill_std_id(
        fclty_type_pos_cd, claim_freq_cd
):
    return str(fclty_type_pos_cd) + str(claim_freq_cd)
