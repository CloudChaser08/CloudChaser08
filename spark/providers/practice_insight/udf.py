

def generate_place_of_service_std_id(
        claim_type,
        pos_cd,
        fclty_type_pos_cd
):
    if claim_type is not None and claim_type == 'P':
        if pos_cd is not None and pos_cd != '':
            return pos_cd
        elif fclty_type_pos_cd is not None and fclty_type_pos_cd != '':
            return fclty_type_pos_cd
    return None
